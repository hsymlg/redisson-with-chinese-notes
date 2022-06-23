/**
 * Copyright (c) 2013-2021 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.LockPubSub;

/**
 * Lock will be removed automatically if client disconnects.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonWriteLock extends RedissonLock implements RLock {

    protected RedissonWriteLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getRawName());
    }

    @Override
    protected String getLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }
    
    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, command,
                            //分支一：锁的模式为空，即当前锁尚未被其他线程持有
                            //当前线程尝试获取写锁时，还没有其他线程成功持有锁，包括读锁和写锁
                            "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                            "if (mode == false) then " +
                                    //利用 hset 命令设置锁模式为写锁
                                  "redis.call('hset', KEYS[1], 'mode', 'write'); " +
                                    //利用 hset 命令为当前线程添加加锁次数记录
                                    //我们可以发现，读写锁中的写锁获取锁不再需要写锁中的加锁超时记录，因为写锁仅支持一个线程来持有锁，锁的超时时间就是线程持有锁的超时时间。
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                                    //利用 pexpire 命令为锁添加过期时间
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                                  "return nil; " +
                              "end; " +
                                    //分支二：锁模式为写锁并且持有写锁为当前线程，当前线程可再次获取写锁
                                    //当前线程重复获取写锁，即读写锁中的写锁支持可重入获取锁
                              "if (mode == 'write') then " +
                                    //锁模式为写锁，利用 hexists 命令判断持有写锁为当前线程
                                  "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                                    //利用 hincrby 命令为当前线程增加1次加锁次数
                                      "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                                        //利用 pttl 获取当前写锁的超时剩余毫秒数
                                      "local currentExpire = redis.call('pttl', KEYS[1]); " +
                                        //利用 pexipre 给锁重新设置锁的过期时间，过期时间为：上次加锁的剩余毫秒数+30000毫秒
                                      "redis.call('pexpire', KEYS[1], currentExpire + ARGV[1]); " +
                                      "return nil; " +
                                  "end; " +
                                "end;" +
                                    //最后：获取锁失败，返回锁pttl
                                "return redis.call('pttl', KEYS[1]);",
                        //KEYS：[“myLock”]
                        //ARGVS：[30_000毫秒,“UUID:threadId:write”]
                        Arrays.<Object>asList(getRawName()),
                        unit.toMillis(leaseTime), getLockName(threadId));
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                "if (mode == false) then " +
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end;" +
                "if (mode == 'write') then " +
                    "local lockExists = redis.call('hexists', KEYS[1], ARGV[3]); " +
                    "if (lockExists == 0) then " +
                        "return nil;" +
                    "else " +
                        "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                        "if (counter > 0) then " +
                            "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                            "return 0; " +
                        "else " +
                            "redis.call('hdel', KEYS[1], ARGV[3]); " +
                            "if (redis.call('hlen', KEYS[1]) == 1) then " +
                                "redis.call('del', KEYS[1]); " +
                                "redis.call('publish', KEYS[2], ARGV[1]); " + 
                            "else " +
                                // has unlocked read-locks
                                "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                            "end; " +
                            "return 1; "+
                        "end; " +
                    "end; " +
                "end; "
                + "return nil;",
        Arrays.<Object>asList(getRawName(), getChannelName()),
        LockPubSub.READ_UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId));
    }
    
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected CompletionStage<Boolean> renewExpirationAsync(long threadId) {
        CompletionStage<Boolean> f = super.renewExpirationAsync(threadId);
        return f.thenCompose(r -> {
            if (!r) {
                RedissonReadLock lock = new RedissonReadLock(commandExecutor, getRawName());
                return lock.renewExpirationAsync(threadId);
            }
            return CompletableFuture.completedFuture(r);
        });
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
              "if (redis.call('hget', KEYS[1], 'mode') == 'write') then " +
                  "redis.call('del', KEYS[1]); " +
                  "redis.call('publish', KEYS[2], ARGV[1]); " +
                  "return 1; " +
              "end; " +
              "return 0; ",
              Arrays.<Object>asList(getRawName(), getChannelName()), LockPubSub.READ_UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.HGET, getRawName(), "mode");
        String res = get(future);
        return "write".equals(res);
    }

}
