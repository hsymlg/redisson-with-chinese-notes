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
                //分支一：锁模式不存在，往锁对应的channel发送消息
                //如果锁模式不存在，那么证明没有线程持有读写锁
                //当前线程即使没有持有锁，但还是调用了释放锁的方法
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                "if (mode == false) then " +
                        //如果锁模式为空，往读写锁对应的channel发送释放锁的消息，然后返回1，lua脚本执行完毕
                        //channel 发布锁释放消息的用处：
                        //其实在 Redisson 提供的各种分布式锁中，不管是可重入锁、公平锁，还是到现在的读写锁，都会利用 redis 的pub/sub机制来做下面的通知机制。
                        //在线程获取锁失败的时候，在等待前会先订阅锁对应的 channel，然后进入等待状态。
                        //如果当前线程成功释放锁，那么会在锁对应的 channel 发布释放锁的消息；假设此时有其他线程在等待获取锁，那么就会接收到 channel 里释放锁的消息，提前跳出等待状态，去获取锁。
                        //关于锁channel，要注意的是：可重入锁和读写锁，等待线程都是订阅同一个channel；而公平锁不是，公平锁是每个等待线程都订阅自己指定的channel，从而做到公平。
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                        //分支二：锁存在，但当前线程没有持有锁
                "local lockExists = redis.call('hexists', KEYS[1], ARGV[2]); " +
                        //利用 hexists 命令判断当前线程是否持有锁
                "if (lockExists == 0) then " +
                        //如果不存在直接返回null，表示释放锁失败
                    "return nil;" +
                "end; " +
                    //分支三：锁存在且当前线程持有锁
                    //利用 hincrby 命令，给当前线程持有锁数量减1
                "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " +
                        //如果持有锁数量减1后等于0(注意当前线程重入是不会进入这个条件的)，证明当前线程不再持有锁，那么利用 hdel 命令将锁map中加锁次数记录删掉
                "if (counter == 0) then " +
                    "redis.call('hdel', KEYS[1], ARGV[2]); " + 
                "end;" +
                        //删除线程持有锁对应的加锁超时记录
                        //为什么给读锁扣减不需要先判断锁的模式？在锁map中记录加锁次数时，读锁的key是UUID:threadId，即客户端ID:线程ID；而写锁的key是UUID:threadId:write，即客户端ID:线程ID:write，那么就是说读锁的key和写锁的key是不一样的。所以解锁的时候，直接使用对应key来扣减持有锁次数即可。
                        //还有一点很重要的是，相同线程，如果获取了写锁后，还是可以继续获取读锁的。所以只需要判断锁map有读锁加锁次数记录即可，就可以判断当前线程是持有读锁的，并不需要关心当前锁的模式。
                "redis.call('del', KEYS[3] .. ':' .. (counter+1)); " +
                //分支四：给当前锁刷新过期时间
                        //当前线程释放一次读锁后，锁map还存在加锁次数记录
                        //有可能是当前线程还持有读锁/写锁，或者当前线程或其他线程同时持有读锁
                        //利用 hlen 获取锁map中key的数量
                        //如果锁map中 key 的数量还是大于1，那么证明还有线程持有锁，遍历锁map集合中的加锁次数key，根据加锁超时记录获取最大的超时时间
                "if (redis.call('hlen', KEYS[1]) > 1) then " +
                        //设置 maxRemainTime 为 -3
                    "local maxRemainTime = -3; " +
                        //利用 hkeys 命令获取锁map中所有key
                    "local keys = redis.call('hkeys', KEYS[1]); " +
                        //遍历
                    "for n, key in ipairs(keys) do " +
                        //利用 hget 命令获取key对应的加锁次数
                        "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                        "if type(counter) == 'number' then " +
                            //遍历加锁次数
                            "for i=counter, 1, -1 do " +
                                //拼接 key 对应的加锁记录对应的超时时间，利用 pttl 获取超时时间
                                "local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i); " +
                                //与 maxRemainTime 对比，获取当前最大的超时时间，赋值给 maxRemainTime
                                "maxRemainTime = math.max(remainTime, maxRemainTime);" + 
                            "end; " + 
                        "end; " + 
                    "end; " +
                            //判断 maxRemainTime 是否大于0，如果大于0，给锁重新设置过期时间为 maxRemainTime，然后返回0结束lua脚本的执行
                    "if maxRemainTime > 0 then " +
                        "redis.call('pexpire', KEYS[1], maxRemainTime); " +
                        "return 0; " +
                    "end;" + 
                        //如果当前读写锁的锁模式是写锁，直接返回0结束lua脚本的执行
                    "if mode == 'write' then " + 
                        "return 0;" + 
                    "end; " +
                "end; " +
                    //当走到最后的操作，证明当前线程不但成功释放锁，并且释放后当前读写锁已经没有其他线程再持有锁了
                    //所以到这里，我们可以直接将读写锁对应的key直接删掉，并且往读写锁对应的channel中发布释放锁消息
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
