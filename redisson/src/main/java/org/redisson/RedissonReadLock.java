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
public class RedissonReadLock extends RedissonLock implements RLock {

    protected RedissonReadLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getRawName());
    }
    
    String getWriteLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }

    String getReadWriteTimeoutNamePrefix(long threadId) {
        return suffixName(getRawName(), getLockName(threadId)) + ":rwlock_timeout";
    }

    //KEYS
    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, command,
                                //利用 hget 命令获取当前锁模式(hget myLock mode)
                                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                                        //当前线程尝试获取读锁时，还没有其他线程成功获取锁，不管是读锁还是写锁
                                "if (mode == false) then " +
                                        //利用 hset 命令设置锁模式为读锁(hset myLock mode read),执行后，锁内容如下：myLock:{"mode":"read"}
                                  "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                                        /*
                                         * 利用 hset 命令为当前线程添加加锁次数记录，hset myLock UUID-1:threadId-1 1，
                                         * 执行后锁的内容如下
                                         * myLock:{
                                         *     "mode":"read",
                                         *     "UUID-1:threadId-1":1}
                                         */
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                                        //利用 set 命令为当前线程添加一条加锁超时记录（set {myLock}:UUID-1:threadId-1:rwlock_timeout:1 1）
                                        //执行后锁的内容如下
                                        //myLock:{
                                        //    "mode":"read",
                                        //    "UUID-1:threadId-1":1
                                        //}
                                        //{myLock}:UUID-1:threadId-1:rwlock_timeout:1 1
                                  "redis.call('set', KEYS[2] .. ':1', 1); " +
                                        //利用 pexpire 命令为锁&当前线程超时记录添加过期时间
                                        //pexpire {myLock}:UUID-1:threadId-1:rwlock_timeout:1 30000
                                        //pexpire myLock 30000
                                  "redis.call('pexpire', KEYS[2] .. ':1', ARGV[1]); " +
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                                  "return nil; " +
                                "end; " +
                                        //锁模式为读锁或锁模式为写锁并且获取写锁为当前线程，当前线程可再次获取读锁
                                        //锁模式为读锁，当前线程可获取读锁。即：redisson提供的读写锁支持不同线程重复获取锁
                                        //锁模式为写锁，并且获取写锁的线程为当前线程，当前线程可获取读锁。即：redisson 提供的读写锁，读写并不是完全互斥，而是支持同一线程先获取写锁再获取读锁。
                                "if (mode == 'read') or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1) then " +
                                        //利用 hincrby 命令为当前线程增加加锁次数
                                        //假设当前线程之前获取过1次锁（假设是读锁），执行后锁内容如下：
                                        //myLock:{
                                        //    "mode":"read",
                                        //    "UUID-1:threadId-1":2
                                        //}
                                  "local ind = redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                                        //为当前线程拼接加锁记录 key，然后利用 set 命令添加一条加锁超时记录
                                        //key = {myLock}:UUID-1:threadId-1:rwlock_timeout:2
                                        //set {myLock}:UUID-1:threadId-1:rwlock_timeout:2 1
                                        //执行后的内容，注意下面这种情况是同一个线程重入读锁的
                                        //myLock:{
                                        //    "mode":"read",
                                        //    "UUID-1:threadId-1":2
                                        //}
                                        //{myLock}:UUID-1:threadId-1:rwlock_timeout:1 1
                                        //{myLock}:UUID-1:threadId-1:rwlock_timeout:2 1
                                  "local key = KEYS[2] .. ':' .. ind;" +
                                  "redis.call('set', key, 1); " +
                                        //给新的加锁超时记录设置过期时间
                                  "redis.call('pexpire', key, ARGV[1]); " +
                                        //最后，pttl 命令获取锁的过期时间，利用 pexipre 给锁重新设置锁的过期时间
                                  "local remainTime = redis.call('pttl', KEYS[1]); " +
                                  "redis.call('pexpire', KEYS[1], math.max(remainTime, ARGV[1])); " +
                                  "return nil; " +
                                "end;" +
                                        //不满足上面的两个分支，当前线程就无法成功获取读锁
                                "return redis.call('pttl', KEYS[1]);",
                /*
                 * getRawName()：锁key
                 * getLockName(threadId)：id:threadId -> 客户端UUID:线程ID -> 假设 UUID-1:threadId-1
                 * suffixName()：{锁key}:UUID:threadId
                 * KEYS：[“myLock”,"{myLock}:UUID-1:threadId-1:rwlock_timeout"]
                 * ARGVS：[30_000毫秒,“UUID-1:threadId-1”,“UUID-1:threadId-1:write”]
                 */
                Arrays.<Object>asList(getRawName(), getReadWriteTimeoutNamePrefix(threadId)),
                        unit.toMillis(leaseTime), getLockName(threadId), getWriteLockName(threadId));
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);

        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                "if (mode == false) then " +
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                "local lockExists = redis.call('hexists', KEYS[1], ARGV[2]); " +
                "if (lockExists == 0) then " +
                    "return nil;" +
                "end; " +
                    
                "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " + 
                "if (counter == 0) then " +
                    "redis.call('hdel', KEYS[1], ARGV[2]); " + 
                "end;" +
                "redis.call('del', KEYS[3] .. ':' .. (counter+1)); " +
                
                "if (redis.call('hlen', KEYS[1]) > 1) then " +
                    "local maxRemainTime = -3; " + 
                    "local keys = redis.call('hkeys', KEYS[1]); " + 
                    "for n, key in ipairs(keys) do " + 
                        "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                        "if type(counter) == 'number' then " + 
                            "for i=counter, 1, -1 do " + 
                                "local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i); " + 
                                "maxRemainTime = math.max(remainTime, maxRemainTime);" + 
                            "end; " + 
                        "end; " + 
                    "end; " +
                            
                    "if maxRemainTime > 0 then " +
                        "redis.call('pexpire', KEYS[1], maxRemainTime); " +
                        "return 0; " +
                    "end;" + 
                        
                    "if mode == 'write' then " + 
                        "return 0;" + 
                    "end; " +
                "end; " +
                    
                "redis.call('del', KEYS[1]); " +
                "redis.call('publish', KEYS[2], ARGV[1]); " +
                "return 1; ",
                //KEYS：[“myLock”,“redisson_rwlock:{myLock}”,"{myLock}:UUID-1:threadId-1:rwlock_timeout","{myLock}"]
                //ARGVS：[0L,“UUID:threadId”]
                Arrays.<Object>asList(getRawName(), getChannelName(), timeoutPrefix, keyPrefix),
                LockPubSub.UNLOCK_MESSAGE, getLockName(threadId));
    }

    protected String getKeyPrefix(long threadId, String timeoutPrefix) {
        return timeoutPrefix.split(":" + getLockName(threadId))[0];
    }

    //重写了RedissonBaseLock的方法
    @Override
    protected CompletionStage<Boolean> renewExpirationAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);
        
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                //第一步，获取当前线程的加锁次数
                "local counter = redis.call('hget', KEYS[1], ARGV[2]); " +
                        //分支一：当前线程获取锁次数大于0，刷新锁过期时间；如果锁集合元素大于1，刷新里面加锁线程的过期时间
                "if (counter ~= false) then " +
                        //利用 pexpire 命令重新刷新锁过期时间
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                    //利用 hlen 命令获取锁集合里面的元素个数，然后判断是否大于1个以上key
                    //做这个判断是因为，如果是可重入锁或者公平锁，锁集合里面只有有一个key，就是当前成功获取锁的线程。
                    //但如果是读写锁，他里面可包含2个以上key，其中一个就是锁的模式，即 mode 字段，可表示当前锁是读锁还是写锁。
                    "if (redis.call('hlen', KEYS[1]) > 1) then " +
                        //利用 hkeys 命令获取锁集合里面所有key
                        "local keys = redis.call('hkeys', KEYS[1]); " + 
                        "for n, key in ipairs(keys) do " +
                        //遍历所有key，并利用 hget 命令来获取key对应的value
                            "counter = tonumber(redis.call('hget', KEYS[1], key)); " +
                            //如果key的值为数字，证明此key是加锁成功的线程，并且value的值表示线程加锁次数；
                            //需要遍历加锁次数利用 pexpire 为这个线程对应的加锁记录刷新过期时间
                            "if type(counter) == 'number' then " +
                                "for i=counter, 1, -1 do " +
                                    //其实就是为了给所有加锁记录刷新过期时间
                                    "redis.call('pexpire', KEYS[2] .. ':' .. key .. ':rwlock_timeout:' .. i, ARGV[1]); " + 
                                "end; " + 
                            "end; " + 
                        "end; " +
                    "end; " +
                    
                    "return 1; " +
                "end; " +
                "return 0;",
            //KEYS：[“myLock”,"{myLock}"]
            //ARGVS：[30_000毫秒,“UUID-1:threadId-1”]
            Arrays.<Object>asList(getRawName(), keyPrefix),
            internalLockLeaseTime, getLockName(threadId));
    }
    
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hget', KEYS[1], 'mode') == 'read') then " +
                    "redis.call('del', KEYS[1]); " +
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                "return 0; ",
                Arrays.<Object>asList(getRawName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.HGET, getRawName(), "mode");
        String res = get(future);
        return "read".equals(res);
    }

}
