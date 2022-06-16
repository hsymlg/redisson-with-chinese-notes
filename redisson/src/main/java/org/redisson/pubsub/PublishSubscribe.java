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

import org.redisson.PubSubEntry;
import org.redisson.client.BaseRedisPubSubListener;
import org.redisson.client.ChannelName;
import org.redisson.client.RedisPubSubListener;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.pubsub.PubSubType;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 
 * @author Nikita Koksharov
 *
 */
//注意，这是个发布订阅的抽象类，对于锁的发布订阅，来自他的继承类LockPubSub
//范型<E extends PubSubEntry<E>> 类似LockPubSub extends PublishSubscribe<RedissonLockEntry> ，E就代表RedissonLockEntry
abstract class PublishSubscribe<E extends PubSubEntry<E>> {

    private final ConcurrentMap<String, E> entries = new ConcurrentHashMap<>();
    private final PublishSubscribeService service;

    PublishSubscribe(PublishSubscribeService service) {
        super();
        this.service = service;
    }

    public void unsubscribe(E entry, String entryName, String channelName) {
        AsyncSemaphore semaphore = service.getSemaphore(new ChannelName(channelName));
        semaphore.acquire(() -> {
            if (entry.release() == 0) {
                entries.remove(entryName);
                service.unsubscribe(PubSubType.UNSUBSCRIBE, new ChannelName(channelName))
                        .whenComplete((r, e) -> {
                            semaphore.release();
                        });
            } else {
                semaphore.release();
            }
        });

    }

    public void timeout(CompletableFuture<?> promise) {
        service.timeout(promise);
    }

    public void timeout(CompletableFuture<?> promise, long timeout) {
        service.timeout(promise, timeout);
    }

    //entryName是uuid+{$KEY}
    //channelName是redisson_lock__channel:{$KEY}
    // 在LockPubSub中注册一个entryName -> RedissonLockEntry的哈希映射，
    // RedissonLockEntry实例中存放着RPromise<RedissonLockEntry>结果，一个信号量形式的锁和订阅方法重入计数器
    public CompletableFuture<E> subscribe(String entryName, String channelName) {
        //pubsubService中有一个简单的无冲突解决的AsyncSemaphore数据结构，这里只是简单从中获取一个
        AsyncSemaphore semaphore = service.getSemaphore(new ChannelName(channelName));
        CompletableFuture<E> newPromise = new CompletableFuture<>();
        //获取一个许可，在提供一个许可前一直将线程阻塞，除非线程被中断。
        semaphore.acquire(() -> {
            //isDone方法检查计算是否完成
            if (newPromise.isDone()) {
                //释放许可，然后返回
                semaphore.release();
                return;
            }

            E entry = entries.get(entryName);
            if (entry != null) {
                entry.acquire();
                semaphore.release();
                entry.getPromise().whenComplete((r, e) -> {
                    if (e != null) {
                        newPromise.completeExceptionally(e);
                        return;
                    }
                    newPromise.complete(r);
                });
                return;
            }
            //创建一个E,在lock中是RedissonLockEntry
            E value = createEntry(newPromise);
            //获得1个许可
            value.acquire();
            //判断entryName是否存在，不存在则插入，返回null；存在返回旧值
            E oldValue = entries.putIfAbsent(entryName, value);
            if (oldValue != null) {
                oldValue.acquire();
                semaphore.release();
                oldValue.getPromise().whenComplete((r, e) -> {
                    if (e != null) {
                        newPromise.completeExceptionally(e);
                        return;
                    }
                    newPromise.complete(r);
                });
                return;
            }

            RedisPubSubListener<Object> listener = createListener(channelName, value);
            CompletableFuture<PubSubConnectionEntry> s = service.subscribeNoTimeout(LongCodec.INSTANCE, channelName, semaphore, listener);
            newPromise.whenComplete((r, e) -> {
                if (e != null) {
                    s.completeExceptionally(e);
                }
            });
            s.whenComplete((r, e) -> {
                if (e != null) {
                    value.getPromise().completeExceptionally(e);
                    return;
                }
                value.getPromise().complete(value);
            });

        });

        return newPromise;
    }

    protected abstract E createEntry(CompletableFuture<E> newPromise);

    protected abstract void onMessage(E value, Long message);

    private RedisPubSubListener<Object> createListener(String channelName, E value) {
        RedisPubSubListener<Object> listener = new BaseRedisPubSubListener() {

            @Override
            public void onMessage(CharSequence channel, Object message) {
                if (!channelName.equals(channel.toString())) {
                    return;
                }

                PublishSubscribe.this.onMessage(value, (Long) message);
            }
        };
        return listener;
    }

}
