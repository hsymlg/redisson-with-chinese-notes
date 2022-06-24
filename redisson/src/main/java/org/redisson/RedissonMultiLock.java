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

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.api.RLockAsync;
import org.redisson.client.RedisResponseTimeoutException;
import org.redisson.misc.CompletableFutureWrapper;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;

/**
 * Groups multiple independent locks and manages them as one lock.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonMultiLock implements RLock {

    class LockState {
        
        private final long newLeaseTime;
        private final long lockWaitTime;
        private final List<RLock> acquiredLocks;
        private final long waitTime;
        private final long threadId;
        private final long leaseTime;
        private final TimeUnit unit;

        private long remainTime;
        private long time = System.currentTimeMillis();
        private int failedLocksLimit;
        
        LockState(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
            this.waitTime = waitTime;
            this.leaseTime = leaseTime;
            this.unit = unit;
            this.threadId = threadId;

            if (leaseTime > 0) {
                if (waitTime > 0) {
                    newLeaseTime = unit.toMillis(waitTime)*2;
                } else {
                    newLeaseTime = unit.toMillis(leaseTime);
                }
            } else {
                newLeaseTime = -1;
            }

            remainTime = -1;
            if (waitTime > 0) {
                remainTime = unit.toMillis(waitTime);
            }
            lockWaitTime = calcLockWaitTime(remainTime);
            
            failedLocksLimit = failedLocksLimit();
            acquiredLocks = new ArrayList<>(locks.size());
        }
        
        CompletionStage<Boolean> tryAcquireLockAsync(ListIterator<RLock> iterator) {
            if (!iterator.hasNext()) {
                return checkLeaseTimeAsync();
            }

            RLock lock = iterator.next();
            RFuture<Boolean> lockAcquiredFuture;
            if (waitTime <= 0 && leaseTime <= 0) {
                lockAcquiredFuture = lock.tryLockAsync(threadId);
            } else {
                long awaitTime = Math.min(lockWaitTime, remainTime);
                lockAcquiredFuture = lock.tryLockAsync(awaitTime, newLeaseTime, TimeUnit.MILLISECONDS, threadId);
            }

            return lockAcquiredFuture
                    .exceptionally(e -> null)
                    .thenCompose(res -> {
                boolean lockAcquired = false;
                if (res != null) {
                    lockAcquired = res;
                } else {
                    unlockInnerAsync(Arrays.asList(lock), threadId);
                }

                if (lockAcquired) {
                    acquiredLocks.add(lock);
                } else {
                    if (locks.size() - acquiredLocks.size() == failedLocksLimit()) {
                        return checkLeaseTimeAsync();
                    }

                    if (failedLocksLimit == 0) {
                        return unlockInnerAsync(acquiredLocks, threadId).thenCompose(r -> {
                            if (waitTime <= 0) {
                                return CompletableFuture.completedFuture(false);
                            }
                            
                            failedLocksLimit = failedLocksLimit();
                            acquiredLocks.clear();
                            // reset iterator
                            while (iterator.hasPrevious()) {
                                iterator.previous();
                            }
                            
                            return checkRemainTimeAsync(iterator);
                        });
                    } else {
                        failedLocksLimit--;
                    }
                }
                
                return checkRemainTimeAsync(iterator);
            });
        }
        
        private CompletableFuture<Boolean> checkLeaseTimeAsync() {
            if (leaseTime > 0) {
                List<CompletableFuture<Boolean>> futures = new ArrayList<>();
                for (RLock rLock : acquiredLocks) {
                    RFuture<Boolean> future = ((RedissonBaseLock) rLock).expireAsync(unit.toMillis(leaseTime), TimeUnit.MILLISECONDS);
                    futures.add(future.toCompletableFuture());
                }
                CompletableFuture<Void> f = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                return f.thenApply(r -> true);
            }

            return CompletableFuture.completedFuture(true);
        }
        
        private CompletionStage<Boolean> checkRemainTimeAsync(ListIterator<RLock> iterator) {
            if (remainTime > 0) {
                remainTime += -(System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                if (remainTime <= 0) {
                    return unlockInnerAsync(acquiredLocks, threadId).thenApply(res -> false);
                }
            }
            
            return tryAcquireLockAsync(iterator);
        }
        
    }
    
    final List<RLock> locks = new ArrayList<>();
    
    /**
     * Creates instance with multiple {@link RLock} objects.
     * Each RLock object could be created by own Redisson instance.
     *
     * @param locks - array of locks
     */
    public RedissonMultiLock(RLock... locks) {
        if (locks.length == 0) {
            throw new IllegalArgumentException("Lock objects are not defined");
        }
        this.locks.addAll(Arrays.asList(locks));
    }
    
    @Override
    public void lock() {
        try {
            lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void lock(long leaseTime, TimeUnit unit) {
        try {
            lockInterruptibly(leaseTime, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit) {
        return lockAsync(leaseTime, unit, Thread.currentThread().getId());
    }
    
    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit, long threadId) {
        long baseWaitTime = locks.size() * 1500;
        long waitTime;
        if (leaseTime <= 0) {
            waitTime = baseWaitTime;
        } else {
            leaseTime = unit.toMillis(leaseTime);
            waitTime = leaseTime;
            if (waitTime <= 2000) {
                waitTime = 2000;
            } else if (waitTime <= baseWaitTime) {
                waitTime = ThreadLocalRandom.current().nextLong(waitTime/2, waitTime);
            } else {
                waitTime = ThreadLocalRandom.current().nextLong(baseWaitTime, waitTime);
            }
        }

        CompletionStage<Void> f = tryLockAsyncCycle(threadId, leaseTime, TimeUnit.MILLISECONDS, waitTime);
        return new CompletableFutureWrapper<>(f);
    }
    
    protected CompletionStage<Void> tryLockAsyncCycle(long threadId, long leaseTime, TimeUnit unit, long waitTime) {
        return tryLockAsync(waitTime, leaseTime, unit, threadId).thenCompose(res -> {
            if (res) {
                return CompletableFuture.completedFuture(null);
            } else {
                return tryLockAsyncCycle(threadId, leaseTime, unit, waitTime);
            }
        });
    }


    @Override
    public void lockInterruptibly() throws InterruptedException {
        lockInterruptibly(-1, null);
    }

    @Override
    public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
        // 连锁的锁数量 * 1500，假设我们这里是3个锁，那么就是3*1500 = 4500 毫秒
        long baseWaitTime = locks.size() * 1500;
        // 死循环，尝试获取锁，直到成功
        while (true) {
            long waitTime;
            if (leaseTime <= 0) {
                // 没有设置剩余时间，waitTime 被赋值为 baseWaitTime
                waitTime = baseWaitTime;
            } else {
                //有剩余时间，waitTime 初始值等于 leaseTime
                waitTime = unit.toMillis(leaseTime);
                if (waitTime <= baseWaitTime) {
                    //如果 waitTIme 小于等于 baseWaitTime，重新赋值为 [waitTime/2,waitTime] 之间的随机数
                    waitTime = ThreadLocalRandom.current().nextLong(waitTime/2, waitTime);
                } else {
                    //如果 waitTime 大于 baseWaitTIme，重新赋值为 [baseWaitTIme,waitTime] 之间的随机数
                    waitTime = ThreadLocalRandom.current().nextLong(baseWaitTime, waitTime);
                }
            }
            // 最后，waitTime 是 [2250,4500] 之间的随机数
            // leaseTime 还是 -1
            if (tryLock(waitTime, leaseTime, TimeUnit.MILLISECONDS)) {
                return;
            }
        }
    }

    @Override
    public boolean tryLock() {
        try {
            return tryLock(-1, -1, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    protected void unlockInner(Collection<RLock> locks) {
        locks.stream()
                .map(RLockAsync::unlockAsync)
                .forEach(f -> {
                    try {
                        f.toCompletableFuture().join();
                    } catch (Exception e) {
                        // skip
                    }
                });
    }
    
    protected RFuture<Void> unlockInnerAsync(Collection<RLock> locks, long threadId) {
        List<CompletableFuture<Void>> futures = new ArrayList<>(locks.size());
        for (RLock lock : locks) {
            RFuture<Void> f = lock.unlockAsync(threadId);
            futures.add(f.toCompletableFuture());
        }
        CompletableFuture<Void> future = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        return new CompletableFutureWrapper<>(future);
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
        return tryLock(waitTime, -1, unit);
    }
    
    protected int failedLocksLimit() {
        return 0;
    }
    
    @Override
    public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
//        try {
//            return tryLockAsync(waitTime, leaseTime, unit).get();
//        } catch (ExecutionException e) {
//            throw new IllegalStateException(e);
//        }
        long newLeaseTime = -1;
        if (leaseTime > 0) {
            if (waitTime > 0) {
                newLeaseTime = unit.toMillis(waitTime)*2;
            } else {
                newLeaseTime = unit.toMillis(leaseTime);
            }
        }
        
        long time = System.currentTimeMillis();
        long remainTime = -1;
        //waiteTime 不是-1，remainTime 赋值为 waitTime
        if (waitTime > 0) {
            remainTime = unit.toMillis(waitTime);
        }
        // 这里计算锁等待时间，RedissonMultiLock 是直接返回传入的 remainTime
        long lockWaitTime = calcLockWaitTime(remainTime);
        // 允许获取锁失败次数，RedissonMultiLock 限制为0，即任何锁都不能获取失败
        int failedLocksLimit = failedLocksLimit();
        // 记录获取成功的锁
        List<RLock> acquiredLocks = new ArrayList<>(locks.size());
        for (ListIterator<RLock> iterator = locks.listIterator(); iterator.hasNext();) {
            RLock lock = iterator.next();
            boolean lockAcquired;
            try {
                if (waitTime <= 0 && leaseTime <= 0) {
                    lockAcquired = lock.tryLock();
                } else {
                    // 获取等待时间，因为 RedissonMultiLock 的处理是 lockWaitTime = remainTime，
                    // 所以 awaitTime 就等于 remainTime，也就是调用方法时传入的 waitTime
                    long awaitTime = Math.min(lockWaitTime, remainTime);
                    // 调用 RedissonLock 的 tryLock(long waitTime, long leaseTime, TimeUnit unit) 方法
                    // 其实就是尝试获取锁，如果获取失败，最长等待时间为 awaitTime，如果获取成功了，持有锁时间最长为 newLeaseTime，
                    // 这里 newLeaseTime 虽然 -1，但并不是锁没有设置过期时间，大家要记得 RedissonLock 的 watchdog 机制哦！
                    lockAcquired = lock.tryLock(awaitTime, newLeaseTime, TimeUnit.MILLISECONDS);
                }
            } catch (RedisResponseTimeoutException e) {
                // 如果redis返回响应超时，释放当前锁，为了兼容成功获取锁，但是redis响应超时的情况。
                unlockInner(Arrays.asList(lock));
                // 加锁结果置为false
                lockAcquired = false;
            } catch (Exception e) {
                // 捕获异常，加锁结果置为false
                lockAcquired = false;
            }
            // 如果获取锁成功，将当前锁加入成功列表中
            if (lockAcquired) {
                acquiredLocks.add(lock);
            } else {
                // 假设 锁数量-成功锁数量等于失败上限，则跳出循环。在 RedissonMultiLock 中不会出现这种情况，主要是为了 RedissonRedLock 服务的，
                // 因为 RedLock 算法中只要一半以上的锁获取成功，就算成功持有锁。
                if (locks.size() - acquiredLocks.size() == failedLocksLimit()) {
                    break;
                }
                // 如果失败上限为0，证明已经没有机会再失败了，执行下面的操作
                if (failedLocksLimit == 0) {
                    // 释放成功获取的锁记录
                    unlockInner(acquiredLocks);
                    // 如果等待时间为-1，直接返回false表示获取连锁失败
                    if (waitTime <= 0) {
                        return false;
                    }
                    // 重置失败上限
                    failedLocksLimit = failedLocksLimit();
                    // 清理成功成功记录
                    acquiredLocks.clear();
                    // reset iterator
                    // 重置锁列表，后续所有锁都需要重新获取
                    while (iterator.hasPrevious()) {
                        iterator.previous();
                    }
                } else {
                    // 如果失败上限不为0，递减1
                    failedLocksLimit--;
                }
            }
            // 如果remainTime不等于-1，即传入的waitTime不为-1
            if (remainTime > 0) {
                // remainTime 减去当前时间减去尝试获取锁时的当前时间
                remainTime -= System.currentTimeMillis() - time;
                // 重置time
                time = System.currentTimeMillis();
                // 如果remainTime小于等于0，即等待时间已经消耗完了，释放所有成功获取的锁记录，返回false表示尝试获取锁失败
                if (remainTime <= 0) {
                    unlockInner(acquiredLocks);
                    return false;
                }
            }
        }
        // 到这里，表示已经成功获取指定数量的锁，判断 leaseTime 是否不为-1，如果不是，即锁需要设定持有时间
        if (leaseTime > 0) {
            // 遍历所有成功获取的锁记录，异步调用 pexpire 为锁key 设置超时时间
            acquiredLocks.stream()
                    .map(l -> (RedissonBaseLock) l)
                    .map(l -> l.expireAsync(unit.toMillis(leaseTime), TimeUnit.MILLISECONDS))
                    // 遍历获取异步调用结果
                    .forEach(f -> f.toCompletableFuture().join());
        }
        
        return true;
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        LockState state = new LockState(waitTime, leaseTime, unit, threadId);
        CompletionStage<Boolean> f = state.tryAcquireLockAsync(locks.listIterator());
        return new CompletableFutureWrapper<>(f);
    }
    
    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
        return tryLockAsync(waitTime, leaseTime, unit, Thread.currentThread().getId());
    }

    
    protected long calcLockWaitTime(long remainTime) {
        return remainTime;
    }

    @Override
    public RFuture<Void> unlockAsync(long threadId) {
        return unlockInnerAsync(locks, threadId);
    }
    
    @Override
    public void unlock() {
        List<RFuture<Void>> futures = new ArrayList<>(locks.size());
        // 遍历锁列表，调用RedissonLock异步释放锁的方法
        for (RLock lock : locks) {
            futures.add(lock.unlockAsync());
        }
        // 遍历异步调用返回的 future，同步等待获取结果
        for (RFuture<Void> future : futures) {
            future.toCompletableFuture().join();
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Void> unlockAsync() {
        return unlockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Boolean> tryLockAsync() {
        return tryLockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Void> lockAsync() {
        return lockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Void> lockAsync(long threadId) {
        return lockAsync(-1, null, threadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long threadId) {
        return tryLockAsync(-1, -1, null, threadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
        return tryLockAsync(waitTime, -1, unit);
    }

    @Override
    public RFuture<Integer> getHoldCountAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean forceUnlock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocked() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public RFuture<Boolean> isLockedAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHeldByThread(long threadId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHeldByCurrentThread() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHoldCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Long> remainTimeToLiveAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long remainTimeToLive() {
        throw new UnsupportedOperationException();
    }

}
