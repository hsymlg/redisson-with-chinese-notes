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

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.redisson.api.BatchOptions;
import org.redisson.api.BatchResult;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.convertor.IntegerReplayConvertor;
import org.redisson.client.protocol.decoder.MapValueDecoder;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.command.CommandBatchService;
import org.redisson.connection.MasterSlaveEntry;
import org.redisson.misc.CompletableFutureWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;

/**
 * Base class for implementing distributed locks
 *
 * @author Danila Varatyntsev
 * @author Nikita Koksharov
 */
public abstract class RedissonBaseLock extends RedissonExpirable implements RLock {

    // ExpirationEntry为锁对象维护的实体，每一个ExpirationEntry中都存了一个线程计数器，用来实现可重入锁
    public static class ExpirationEntry {

        // 线程ID -> 线程重入的次数
        private final Map<Long, Integer> threadIds = new LinkedHashMap<>();
        private volatile Timeout timeout;

        public ExpirationEntry() {
            super();
        }

        // 这个方法主要记录线程重入的计数
        public synchronized void addThreadId(long threadId) {
            threadIds.compute(threadId, (t, counter) -> {
                counter = Optional.ofNullable(counter).orElse(0);
                counter++;
                return counter;
            });
        }
        public synchronized boolean hasNoThreads() {
            return threadIds.isEmpty();
        }
        public synchronized Long getFirstThreadId() {
            if (threadIds.isEmpty()) {
                return null;
            }
            return threadIds.keySet().iterator().next();
        }
        public synchronized void removeThreadId(long threadId) {
            threadIds.compute(threadId, (t, counter) -> {
                if (counter == null) {
                    return null;
                }
                counter--;
                if (counter == 0) {
                    return null;
                }
                return counter;
            });
        }

        public void setTimeout(Timeout timeout) {
            this.timeout = timeout;
        }
        public Timeout getTimeout() {
            return timeout;
        }

    }

    private static final Logger log = LoggerFactory.getLogger(RedissonBaseLock.class);

    // 存放entryName -> ExpirationEntry，用于获取当前entryName的线程重入计数器和续期任务
    private static final ConcurrentMap<String, ExpirationEntry> EXPIRATION_RENEWAL_MAP = new ConcurrentHashMap<>();
    protected long internalLockLeaseTime;

    // ID，唯一标识，是一个UUID
    final String id;
    final String entryName;

    // 命令异步执行器实例
    final CommandAsyncExecutor commandExecutor;

    public RedissonBaseLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.id = commandExecutor.getConnectionManager().getId();
        this.internalLockLeaseTime = commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout();
        this.entryName = id + ":" + name;
    }

    protected String getEntryName() {
        return entryName;
    }

    protected String getLockName(long threadId) {
        return id + ":" + threadId;
    }

    // 处理续期
    private void renewExpiration() {
        // 根据entryName获取ExpirationEntry实例，如果为空，说明在cancelExpirationRenewal()方法已经被移除，一般是解锁的时候触发
        ExpirationEntry ee = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (ee == null) {
            return;
        }
        // 新建一个定时任务，这个就是看门狗的实现，io.netty.util.Timeout是Netty结合时间轮使用的定时任务实例
        Timeout task = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                // 这里是重复外面的那个逻辑
                ExpirationEntry ent = EXPIRATION_RENEWAL_MAP.get(getEntryName());
                if (ent == null) {
                    return;
                }
                // 获取ExpirationEntry中首个线程ID，如果为空说明调用过cancelExpirationRenewal()方法清空持有的线程重入计数，一般是锁已经释放的场景
                Long threadId = ent.getFirstThreadId();
                if (threadId == null) {
                    return;
                }
                // 向Redis异步发送续期的命令
                CompletionStage<Boolean> future = renewExpirationAsync(threadId);
                future.whenComplete((res, e) -> {
                    // 抛出异常，续期失败，只打印日志和直接终止任务
                    if (e != null) {
                        log.error("Can't update lock " + getRawName() + " expiration", e);
                        EXPIRATION_RENEWAL_MAP.remove(getEntryName());
                        return;
                    }
                    // 返回true证明续期成功，则递归调用续期方法（重新调度自己），续期失败说明对应的锁已经不存在，直接返回，不再递归
                    if (res) {
                        // reschedule itself
                        renewExpiration();
                    } else {
                        //redis里面的键不存在,关闭续约
                        cancelExpirationRenewal(null);
                    }
                });
            }// 这里的执行频率为leaseTime转换为ms单位下的三分之一，由于leaseTime初始值为-1的情况下才会进入续期逻辑，
            // 那么这里的执行频率为lockWatchdogTimeout的三分之一,如果用户不传键值的有效期的话，默认为30s，
            // 通过下面代码可以看到该任务平均10s执行一次，也就是线程加锁后是10s续命一次。
        }, internalLockLeaseTime / 3, TimeUnit.MILLISECONDS);
        // ExpirationEntry实例持有调度任务实例
        ee.setTimeout(task);
    }

    // 基于线程ID定时调度和续期
    protected void scheduleExpirationRenewal(long threadId) {
        // 新建一个ExpirationEntry记录线程重入计数
        ExpirationEntry entry = new ExpirationEntry();
        ExpirationEntry oldEntry = EXPIRATION_RENEWAL_MAP.putIfAbsent(getEntryName(), entry);
        if (oldEntry != null) {
            // 当前进行的当前线程重入加锁
            oldEntry.addThreadId(threadId);
        } else {
            // 当前进行的当前线程首次加锁
            entry.addThreadId(threadId);
            try {
                // 首次新建ExpirationEntry需要触发续期方法，记录续期的任务句柄
                renewExpiration();
            } finally {
                if (Thread.currentThread().isInterrupted()) {
                    cancelExpirationRenewal(threadId);
                }
            }
        }
    }

    //如果该键值存在(仍在加锁/仍在执行锁内业务)时，重新设置Redis中该键值的失效时间internalLockLeaseTime(默认为30s)，并返回1
    //如果检测到Redis中不存在该键值，则直接返回0。
    protected CompletionStage<Boolean> renewExpirationAsync(long threadId) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return 1; " +
                        "end; " +
                        "return 0;",
                Collections.singletonList(getRawName()),
                internalLockLeaseTime, getLockName(threadId));
    }

    protected void cancelExpirationRenewal(Long threadId) {
        ExpirationEntry task = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (task == null) {
            return;
        }
        
        if (threadId != null) {
            task.removeThreadId(threadId);
        }

        if (threadId == null || task.hasNoThreads()) {
            Timeout timeout = task.getTimeout();
            if (timeout != null) {
                // 如果有续约的定时任务，直接关闭
                timeout.cancel();
            }
            EXPIRATION_RENEWAL_MAP.remove(getEntryName());
        }
    }

    protected <T> RFuture<T> evalWriteAsync(String key, Codec codec, RedisCommand<T> evalCommandType, String script, List<Object> keys, Object... params) {
        MasterSlaveEntry entry = commandExecutor.getConnectionManager().getEntry(getRawName());
        int availableSlaves = entry.getAvailableSlaves();

        CommandBatchService executorService = createCommandBatchService(availableSlaves);
        RFuture<T> result = executorService.evalWriteAsync(key, codec, evalCommandType, script, keys, params);
        if (commandExecutor instanceof CommandBatchService) {
            return result;
        }

        RFuture<BatchResult<?>> future = executorService.executeAsync();
        CompletionStage<T> f = future.handle((res, ex) -> {
            if (ex != null) {
                throw new CompletionException(ex);
            }
            if (commandExecutor.getConnectionManager().getCfg().isCheckLockSyncedSlaves()
                    && res.getSyncedSlaves() < availableSlaves) {
                throw new CompletionException(
                        new IllegalStateException("Only " + res.getSyncedSlaves() + " of " + availableSlaves + " slaves were synced"));
            }

            return commandExecutor.getNow(result.toCompletableFuture());
        });
        return new CompletableFutureWrapper<>(f);
    }

    private CommandBatchService createCommandBatchService(int availableSlaves) {
        if (commandExecutor instanceof CommandBatchService) {
            return (CommandBatchService) commandExecutor;
        }

        BatchOptions options = BatchOptions.defaults()
                                            .syncSlaves(availableSlaves, 1, TimeUnit.SECONDS);

        return new CommandBatchService(commandExecutor, options);
    }

    protected void acquireFailed(long waitTime, TimeUnit unit, long threadId) {
        commandExecutor.get(acquireFailedAsync(waitTime, unit, threadId));
    }

    protected void trySuccessFalse(long currentThreadId, CompletableFuture<Boolean> result) {
        acquireFailedAsync(-1, null, currentThreadId).whenComplete((res, e) -> {
            if (e == null) {
                result.complete(false);
            } else {
                result.completeExceptionally(e);
            }
        });
    }

    protected CompletableFuture<Void> acquireFailedAsync(long waitTime, TimeUnit unit, long threadId) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Condition newCondition() {
        // TODO implement
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocked() {
        return isExists();
    }
    
    @Override
    public RFuture<Boolean> isLockedAsync() {
        return isExistsAsync();
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return isHeldByThread(Thread.currentThread().getId());
    }

    @Override
    public boolean isHeldByThread(long threadId) {
        RFuture<Boolean> future = commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.HEXISTS, getRawName(), getLockName(threadId));
        return get(future);
    }

    private static final RedisCommand<Integer> HGET = new RedisCommand<Integer>("HGET", new MapValueDecoder(), new IntegerReplayConvertor(0));
    
    public RFuture<Integer> getHoldCountAsync() {
        return commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, HGET, getRawName(), getLockName(Thread.currentThread().getId()));
    }
    
    @Override
    public int getHoldCount() {
        return get(getHoldCountAsync());
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return forceUnlockAsync();
    }

    @Override
    public RFuture<Void> unlockAsync() {
        long threadId = Thread.currentThread().getId();
        return unlockAsync(threadId);
    }

    @Override
    public RFuture<Void> unlockAsync(long threadId) {
        // 真正的内部解锁的方法，执行解锁的Lua脚本
        RFuture<Boolean> future = unlockInnerAsync(threadId);
        // 返回的RFuture如果持有的结果为true，说明解锁成功，返回NULL说明线程ID异常，加锁和解锁的客户端线程不是同一个线程
        CompletionStage<Void> f = future.handle((opStatus, e) -> {
            // 取消看门狗的续期任务
            cancelExpirationRenewal(threadId);

            if (e != null) {
                throw new CompletionException(e);
            }
            if (opStatus == null) {
                IllegalMonitorStateException cause = new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                        + id + " thread-id: " + threadId);
                throw new CompletionException(cause);
            }

            return null;
        });

        return new CompletableFutureWrapper<>(f);
    }

    protected abstract RFuture<Boolean> unlockInnerAsync(long threadId);
}
