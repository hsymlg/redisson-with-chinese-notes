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

import java.util.List;

import org.redisson.api.RLock;

/**
 * RedLock locking algorithm implementation for multiple locks. 
 * It manages all locks as one.
 * 
 * @see <a href="http://redis.io/topics/distlock">http://redis.io/topics/distlock</a>
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonRedLock extends RedissonMultiLock {

    /**
     * Creates instance with multiple {@link RLock} objects.
     * Each RLock object could be created by own Redisson instance.
     *
     * @param locks - array of locks
     */
    public RedissonRedLock(RLock... locks) {
        super(locks);
    }

    //重写这个方法是因为，在 RedissonMultiLock 中，是要保证所有子锁都获取成功，连锁才算获取成功，所以 RedissonMultiLock 的 failedLocksLimit() 方法返回的是0。
    //而 RedissonRedLock 重写后，失败上限就是locks.size()/2 + 1，即 RedLock 算法中，大多数节点成功获取锁即可。
    @Override
    protected int failedLocksLimit() {
        return locks.size() - minLocksAmount(locks);
    }
    
    protected int minLocksAmount(final List<RLock> locks) {
        return locks.size()/2 + 1;
    }

    //RedissonMultiLock 遍历锁列表获取锁时，每个锁调用 tryLock 的等待时间都是同样的，即[baseTime/2,baseTime]之间的随机数。
    //其中 baseWaitTime = locks.size() * 1500。因为 RedissonMultiLock 的 calcLockWaitTime 方法就是传入什么返回什么。
    //而 RedissonRedLock 重写后，是每个锁平分这个等待时间，如果均分后小于1，那就是1。
    //我觉得这个重写，最大的意义在于能快速到下一个redis节点获取锁，因为 RedLock 算法只需保证大多数节点成功获取锁即可，避免等待多余的时间。
    @Override
    protected long calcLockWaitTime(long remainTime) {
        return Math.max(remainTime / locks.size(), 1);
    }

    //直接调用 RedissonMultiLock 的 unlockInner() 方法。
    @Override
    public void unlock() {
        unlockInner(locks);
    }

}
