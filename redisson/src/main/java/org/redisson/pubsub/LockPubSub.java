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
package org.redisson.pubsub;

import org.redisson.RedissonLockEntry;

import java.util.concurrent.CompletableFuture;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class LockPubSub extends PublishSubscribe<RedissonLockEntry> {

    public static final Long UNLOCK_MESSAGE = 0L;
    public static final Long READ_UNLOCK_MESSAGE = 1L;

    public LockPubSub(PublishSubscribeService service) {
        super(service);
    }
    
    @Override
    protected RedissonLockEntry createEntry(CompletableFuture<RedissonLockEntry> newPromise) {
        return new RedissonLockEntry(newPromise);
    }

    @Override
    protected void onMessage(RedissonLockEntry value, Long message) {
        if (message.equals(UNLOCK_MESSAGE)) {
            // 从监听器队列取监听线程执行监听回调；获取并移除此队列的头，如果此队列为空，则返回 null。
            Runnable runnableToExecute = value.getListeners().poll();
            if (runnableToExecute != null) {
                runnableToExecute.run();
            }
            // getLatch()返回的是Semaphore，信号量，此处是释放信号量
            // 释放信号量后会唤醒等待的entry.getLatch().tryAcquire(lock或者trylock方法里面)去再次尝试申请锁
            value.getLatch().release();
        } else if (message.equals(READ_UNLOCK_MESSAGE)) {
            while (true) {
                Runnable runnableToExecute = value.getListeners().poll();
                if (runnableToExecute == null) {
                    break;
                }
                runnableToExecute.run();
            }

            value.getLatch().release(value.getLatch().getQueueLength());
        }
    }

}
