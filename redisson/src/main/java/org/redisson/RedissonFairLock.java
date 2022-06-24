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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.LockPubSub;

/**
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * <p>
 * Implements a <b>fair</b> locking so it guarantees an acquire order by threads.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonFairLock extends RedissonLock implements RLock {

    private final long threadWaitTime;
    private final CommandAsyncExecutor commandExecutor;
    private final String threadsQueueName;
    private final String timeoutSetName;

    public RedissonFairLock(CommandAsyncExecutor commandExecutor, String name) {
        this(commandExecutor, name, 60000*5);
    }

    public RedissonFairLock(CommandAsyncExecutor commandExecutor, String name, long threadWaitTime) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.threadWaitTime = threadWaitTime;
        threadsQueueName = prefixName("redisson_lock_queue", name);
        timeoutSetName = prefixName("redisson_lock_timeout", name);
    }

    @Override
    protected CompletableFuture<RedissonLockEntry> subscribe(long threadId) {
        return pubSub.subscribe(getEntryName() + ":" + threadId,
                getChannelName() + ":" + getLockName(threadId));
    }

    @Override
    protected void unsubscribe(RedissonLockEntry entry, long threadId) {
        pubSub.unsubscribe(entry, getEntryName() + ":" + threadId,
                getChannelName() + ":" + getLockName(threadId));
    }

    @Override
    protected CompletableFuture<Void> acquireFailedAsync(long waitTime, TimeUnit unit, long threadId) {
        long wait = threadWaitTime;
        if (waitTime > 0) {
            wait = unit.toMillis(waitTime);
        }

        RFuture<Void> f = evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_VOID,
                // get the existing timeout for the thread to remove
                "local queue = redis.call('lrange', KEYS[1], 0, -1);" +
                        // find the location in the queue where the thread is
                        "local i = 1;" +
                        "while i <= #queue and queue[i] ~= ARGV[1] do " +
                        "i = i + 1;" +
                        "end;" +
                        // go to the next index which will exist after the current thread is removed
                        "i = i + 1;" +
                        // decrement the timeout for the rest of the queue after the thread being removed
                        "while i <= #queue do " +
                        "redis.call('zincrby', KEYS[2], -tonumber(ARGV[2]), queue[i]);" +
                        "i = i + 1;" +
                        "end;" +
                        // remove the thread from the queue and timeouts set
                        "redis.call('zrem', KEYS[2], ARGV[1]);" +
                        "redis.call('lrem', KEYS[1], 0, ARGV[1]);",
                Arrays.asList(threadsQueueName, timeoutSetName),
                getLockName(threadId), wait);
        return f.toCompletableFuture();
    }

    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        long wait = threadWaitTime;
        if (waitTime > 0) {
            wait = unit.toMillis(waitTime);
        }

        long currentTime = System.currentTimeMillis();
        if (command == RedisCommands.EVAL_NULL_BOOLEAN) {
            return evalWriteAsync(getRawName(), LongCodec.INSTANCE, command,
                    // remove stale threads
                    //分支一：清理过期的等待线程
                    //这个死循环的作用主要用于清理过期的等待线程，主要避免下面场景，避免无效客户端占用等待队列资源
                    //获取锁失败，然后进入等待队列，但是网络出现问题，那么后续很有可能就不能继续正常获取锁了。
                    //获取锁失败，然后进入等待队列，但是之后客户端所在服务器宕机了。
                    //开启死循环
                    "while true do " +
                            //利用 lindex 命令判断等待队列中第一个元素是否存在，如果存在，直接跳出循环
                        "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);" +
                        "if firstThreadId2 == false then " +
                            "break;" +
                        "end;" +
                            //如果等待队列中第一个元素不为空（例如返回了LockName，即客户端UUID拼接线程ID），
                            //利用 zscore 在 超时记录集合(sorted set) 中获取对应的超时时间
                        "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));" +
                        "if timeout <= tonumber(ARGV[3]) then " +
                            // remove the item from the queue and timeout set
                            // NOTE we do not alter any other timeout
                            //如果超时时间已经小于当前时间，那么首先从超时集合中移除该节点，接着也在等待队列中弹出第一个节点
                            "redis.call('zrem', KEYS[3], firstThreadId2);" +
                            "redis.call('lpop', KEYS[2]);" +
                        "else " +
                            //如果等待队列中的第一个元素还未超时，直接退出死循环
                            "break;" +
                        "end;" +
                    "end;" +
                    //分支二：检查是否可成功获取锁
                    //其他客户端刚释放锁，并且等待队列为空
                    //其他客户端刚释放锁，并且等待队列中的第一个元素就是当前客户端当前线程
                            //当前锁还未被获取 and（等待队列不存在 or 等待队列的第一个元素是当前客户端当前线程）
                            //exists myLock：判断锁是否存在
                            //exists redisson_lock_queue:{myLock}：判断等待队列是否为空
                            //lindex redisson_lock_timeout:{myLock} 0：获取等待队列中的第一个元素，用于判断是否等于当前客户端当前线程
                    "if (redis.call('exists', KEYS[1]) == 0) " +
                        "and ((redis.call('exists', KEYS[2]) == 0) " +
                            "or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +
                            //从等待队列和超时集合中移除当前线程
                            //lpop redisson_lock_queue:{myLock}：弹出等待队列中的第一个元素，即当前线程
                            //zrem redisson_lock_timeout:{myLock} UUID:threadId：从超时集合中移除当前客户端当前线程
                        "redis.call('lpop', KEYS[2]);" +
                        "redis.call('zrem', KEYS[3], ARGV[2]);" +

                        // decrease timeouts for all waiting in the queue
                            //刷新超时集合中，其他元素的超时时间，即更新他们得分数
                            //zrange redisson_lock_timeout:{myLock} 0 -1：从超时集合中获取所有的元素
                        "local keys = redis.call('zrange', KEYS[3], 0, -1);" +
                        "for i = 1, #keys, 1 do " +
                            //遍历，然后执行下面命令更新分数，即超时时间
                            //因为这里的客户端都是调用 lock()方法，就是等待直到最后获取到锁；
                            //所以某个客户端可以成功获取锁的时候，要帮其他等待的客户端刷新一下等待时间，不然在分支一的死循环中就被干掉了。
                            "redis.call('zincrby', KEYS[3], -tonumber(ARGV[4]), keys[i]);" +
                        "end;" +
                        //最后，往加锁集合(map) myLock 中加入当前客户端当前线程，加锁次数为1(主要用于支持可重入加锁)，然后刷新 myLock 的过期时间，返回nil
                        "redis.call('hset', KEYS[1], ARGV[2], 1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +
                            //分支三：当前线程曾经获取锁，重复获取锁。
                            //当前线程已经成功获取过锁，现在重新再次获取锁。
                            //即：Redisson 的公平锁是支持可重入的。
                            //利用 hexists 命令判断加锁记录集合中，是否存在当前客户端当前线程
                    "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                            //如果存在，那么增加加锁次数，并且刷新锁的过期时间
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +
                    "return 1;",
                    //KEYS：[“myLock”,“redisson_lock_queue:{myLock}”,“redisson_lock_timeout:{myLock}”]
                    //ARGVS：[30_000毫秒,“UUID:threadId”,30_0000毫秒,当前时间戳]
                    Arrays.asList(getRawName(), threadsQueueName, timeoutSetName),
                    unit.toMillis(leaseTime), getLockName(threadId), currentTime, wait);
        }

        if (command == RedisCommands.EVAL_LONG) {
            return evalWriteAsync(getRawName(), LongCodec.INSTANCE, command,
                    // remove stale threads
                    "while true do " +
                        "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);" +
                        "if firstThreadId2 == false then " +
                            "break;" +
                        "end;" +

                        "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));" +
                        "if timeout <= tonumber(ARGV[4]) then " +
                            // remove the item from the queue and timeout set
                            // NOTE we do not alter any other timeout
                            "redis.call('zrem', KEYS[3], firstThreadId2);" +
                            "redis.call('lpop', KEYS[2]);" +
                        "else " +
                            "break;" +
                        "end;" +
                    "end;" +

                    // check if the lock can be acquired now
                    "if (redis.call('exists', KEYS[1]) == 0) " +
                        "and ((redis.call('exists', KEYS[2]) == 0) " +
                            "or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +

                        // remove this thread from the queue and timeout set
                        "redis.call('lpop', KEYS[2]);" +
                        "redis.call('zrem', KEYS[3], ARGV[2]);" +

                        // decrease timeouts for all waiting in the queue
                        "local keys = redis.call('zrange', KEYS[3], 0, -1);" +
                        "for i = 1, #keys, 1 do " +
                            "redis.call('zincrby', KEYS[3], -tonumber(ARGV[3]), keys[i]);" +
                        "end;" +

                        // acquire the lock and set the TTL for the lease
                        "redis.call('hset', KEYS[1], ARGV[2], 1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +

                    // check if the lock is already held, and this is a re-entry
                    "if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2],1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +

                    // the lock cannot be acquired
                    // check if the thread is already in the queue
                            //分支四：当前线程本就在等待队列中，返回等待时间
                            //利用 zscore 获取当前线程在超时集合中的超时时间
                    "local timeout = redis.call('zscore', KEYS[3], ARGV[2]);" +
                    "if timeout ~= false then " +
                        // the real timeout is the timeout of the prior thread
                        // in the queue, but this is approximately correct, and
                        // avoids having to traverse the queue
                            //返回实际的等待时间为：超时集合里的时间戳-30w毫秒-当前时间戳
                        "return timeout - tonumber(ARGV[3]) - tonumber(ARGV[4]);" +
                    "end;" +

                    // add the thread to the queue at the end, and set its timeout in the timeout set to the timeout of
                    // the prior thread in the queue (or the timeout of the lock if the queue is empty) plus the
                    // threadWaitTime
                            //分支五：当前线程首次尝试获取锁，将当前线程加入到超时集合中，同时放入等待队列中
                            //利用 lindex 命令获取等待队列中排在最后的线程
                    "local lastThreadId = redis.call('lindex', KEYS[2], -1);" +
                    "local ttl;" +
                            //如果等待队列中最后的线程不为空且不是当前线程，根据此线程计算出ttl
                    "if lastThreadId ~= false and lastThreadId ~= ARGV[2] then " +
                        "ttl = tonumber(redis.call('zscore', KEYS[3], lastThreadId)) - tonumber(ARGV[4]);" +
                    "else " +
                            //如果等待队列中不存在其他的等待线程，直接返回锁key的过期时间
                        "ttl = redis.call('pttl', KEYS[1]);" +
                    "end;" +
                            //计算timeout，并将当前线程放入超时集合和等待队列中
                    "local timeout = ttl + tonumber(ARGV[3]) + tonumber(ARGV[4]);" +
                    "if redis.call('zadd', KEYS[3], timeout, ARGV[2]) == 1 then " +
                        "redis.call('rpush', KEYS[2], ARGV[2]);" +
                    "end;" +
                            //最后返回ttl
                    "return ttl;",
                    Arrays.asList(getRawName(), threadsQueueName, timeoutSetName),
                    unit.toMillis(leaseTime), getLockName(threadId), wait, currentTime);
        }

        throw new IllegalArgumentException();
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // remove stale threads
                //分支一：清理过期的等待线程
                //和获取锁的第一步一样，开个死循环清理过期的等待线程，主要避免下面场景，避免无效客户端占用等待队列资源
                //获取锁失败，然后进入等待队列，但是网络出现问题，那么后续很有可能就不能继续正常获取锁了。
                //获取锁失败，然后进入等待队列，但是之后客户端所在服务器宕机了。
                //开启死循环
                "while true do "
                        //利用 lindex 命令判断等待队列中第一个元素是否存在，如果存在，直接跳出循环
                + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                + "if firstThreadId2 == false then "
                    + "break;"
                + "end; "
                        //如果等待队列中第一个元素不为空（例如返回了LockName，即客户端UUID拼接线程ID）
                        //利用 zscore 在 超时记录集合(sorted set) 中获取对应的超时时间
                + "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));"
                + "if timeout <= tonumber(ARGV[4]) then "
                        //如果超时时间已经小于当前时间，那么首先从超时集合中移除该节点，接着也在等待队列中弹出第一个节点
                    + "redis.call('zrem', KEYS[3], firstThreadId2); "
                    + "redis.call('lpop', KEYS[2]); "
                        //如果等待队列中的第一个元素还未超时，直接退出死循环
                + "else "
                    + "break;"
                + "end; "
              + "end;"
                //分支二：锁已经被释放，通知等待队列中第一个线程
                        //成功获取锁线程重复调用释放锁的方法，第二次释放时，锁已不存在，就去通知等待队列中的第一个元素
                        //又或者一个极端场景：当前线程未能成功获取锁，但是调用了释放锁的方法，并且刚好此时锁被释放
                        //利用 exists 命令判断锁是否存在
              + "if (redis.call('exists', KEYS[1]) == 0) then " +
                        //如果锁不存在，利用 lidex 获取等待队列中的第一个元素
                    "local nextThreadId = redis.call('lindex', KEYS[2], 0); " +
                        //如果等待列表中第一个元素不为空，即还存在等待线程，往等待线程的订阅channel发送消息，通知其可以尝试获取锁了
                        //RedissonLock 所有等待线程都是订阅锁的同一个channel：redisson_lock__channel:{myLock}，
                        //当有线程释放锁的时候，会往这个通道发送消息，此时所有等待现成都可以订阅消费这条消息，从而从等待状态中释放出来，重新尝试获取锁。
                        //而 RedissonFairLock 不太一样，因为它要支持公平获取锁，即先到先得。所以每个等待线程订阅的都是不同的channel：redisson_lock__channel:{myLock}:UUID:threadId。
                        //当某个线程释放锁的时候，只会往等待队列中第一个线程对应订阅的channel发送消息。
                    "if nextThreadId ~= false then " +
                        "redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                    "end; " +
                    "return 1; " +
                "end;" +
                        //分支三：加锁记录中的线程不是当前线程
                        //当前线程未能成功获取锁，但是调用了释放锁的方法
                        //利用 hexists 命令判断加锁记录集合中，是否存在当前客户端当前线程
                        //加锁记录不存在当前线程，返回nil
                        //如果返回null，会打印相关日志，并调用 tryFailure 方法。
                "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                    "return nil;" +
                "end; " +
                "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                "if (counter > 0) then " +
                    "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                    "return 0; " +
                "end; " +
                    
                "redis.call('del', KEYS[1]); " +
                "local nextThreadId = redis.call('lindex', KEYS[2], 0); " + 
                "if nextThreadId ~= false then " +
                    "redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                "end; " +
                "return 1; ",
                //KEYS：[“myLock”,“redisson_lock_queue:{myLock}”,“redisson_lock_timeout:{myLock}”,“redisson_lock__channel:{myLock}”]
                //ARGVS：[0L,3w毫秒,“UUID:threadId”,当前时间时间戳]
                Arrays.asList(getRawName(), threadsQueueName, timeoutSetName, getChannelName()),
                LockPubSub.UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId), System.currentTimeMillis());
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return deleteAsync(getRawName(), threadsQueueName, timeoutSetName);
    }

    @Override
    public RFuture<Long> sizeInMemoryAsync() {
        List<Object> keys = Arrays.asList(getRawName(), threadsQueueName, timeoutSetName);
        return super.sizeInMemoryAsync(keys);
    }

    @Override
    public RFuture<Boolean> expireAsync(long timeToLive, TimeUnit timeUnit, String param, String... keys) {
        return super.expireAsync(timeToLive, timeUnit, param, getRawName(), threadsQueueName, timeoutSetName);
    }

    @Override
    protected RFuture<Boolean> expireAtAsync(long timestamp, String param, String... keys) {
        return super.expireAtAsync(timestamp, param, getRawName(), threadsQueueName, timeoutSetName);
    }

    @Override
    public RFuture<Boolean> clearExpireAsync() {
        return clearExpireAsync(getRawName(), threadsQueueName, timeoutSetName);
    }

    
    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // remove stale threads
                "while true do "
                + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                + "if firstThreadId2 == false then "
                    + "break;"
                + "end; "
                + "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));"
                + "if timeout <= tonumber(ARGV[2]) then "
                    + "redis.call('zrem', KEYS[3], firstThreadId2); "
                    + "redis.call('lpop', KEYS[2]); "
                + "else "
                    + "break;"
                + "end; "
              + "end;"
                + 
                
                "if (redis.call('del', KEYS[1]) == 1) then " + 
                    "local nextThreadId = redis.call('lindex', KEYS[2], 0); " + 
                    "if nextThreadId ~= false then " +
                        "redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                    "end; " + 
                    "return 1; " + 
                "end; " + 
                "return 0;",
                Arrays.asList(getRawName(), threadsQueueName, timeoutSetName, getChannelName()),
                LockPubSub.UNLOCK_MESSAGE, System.currentTimeMillis());
    }

}