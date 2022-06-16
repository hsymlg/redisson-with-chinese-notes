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
package org.redisson.micronaut.cache;

import io.micronaut.cache.AsyncCache;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.ArgumentUtils;
import org.redisson.api.RMap;
import org.redisson.api.RMapCache;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonAsyncCache implements AsyncCache<RMap<Object, Object>> {

    private final ConversionService<?> conversionService;
    private final RMapCache<Object, Object> mapCache;
    private final RMap<Object, Object> map;
    private final ExecutorService executorService;

    public RedissonAsyncCache(RMapCache<Object, Object> mapCache,
                              RMap<Object, Object> map,
                              ExecutorService executorService,
                              ConversionService<?> conversionService) {
        this.mapCache = mapCache;
        this.map = map;
        this.executorService = executorService;
        this.conversionService = conversionService;
    }

    @Override
    public <T> CompletableFuture<Optional<T>> get(Object key, Argument<T> requiredType) {
        ArgumentUtils.requireNonNull("key", key);
        return map.getAsync(key)
                      .thenApply(v -> {
                          if (v != null) {
                              return Optional.of((T)conversionService.convert(v, ConversionContext.of(requiredType)));
                          }
                          return Optional.<T>empty();
                      })
                      .toCompletableFuture();
    }

    @Override
    public <T> CompletableFuture<T> get(Object key, Argument<T> requiredType, Supplier<T> supplier) {
        ArgumentUtils.requireNonNull("key", key);
        return get(key, requiredType).thenCompose(existingValue -> {
            if (existingValue.isPresent()) {
                return CompletableFuture.completedFuture(existingValue.get());
            } else {
                //以Async结尾并且没有指定Executor的方法会使用ForkJoinPool.commonPool()作为它的线程池执行异步代码
                //supplyAsync表示创建带返回值的异步任务的，相当于ExecutorService submit(Callable<T> task) 方法，
                //runAsync表示创建无返回值的异步任务，相当于ExecutorService submit(Runnable task)方法，这两方法的效果跟submit是一样的
                return CompletableFuture.supplyAsync(supplier, executorService)
                                        //thenApply 表示某个任务执行完成后执行的动作，即回调方法，会将该任务的执行结果即方法返回值作为入参传递到回调方法中
                                        .thenApply(value -> {
                                            put(key, value);
                                            return value;
                                        });
            }
        });
    }

    @Override
    public <T> CompletableFuture<Optional<T>> putIfAbsent(Object key, T value) {
        ArgumentUtils.requireNonNull("key", key);
        ArgumentUtils.requireNonNull("value", value);
        return map.putIfAbsentAsync(key, value)
                        .thenApply(v -> Optional.ofNullable((T) v))
                        .toCompletableFuture();
    }

    @Override
    public CompletableFuture<Boolean> put(Object key, Object value) {
        ArgumentUtils.requireNonNull("key", key);
        ArgumentUtils.requireNonNull("value", value);
        return map.fastPutAsync(key, value)
                        .thenApply(counter -> true)
                        .toCompletableFuture();
    }

    @Override
    public CompletableFuture<Boolean> invalidate(Object key) {
        ArgumentUtils.requireNonNull("key", key);
        return map.fastRemoveAsync(key)
                    .thenApply(counter -> true)
                    .toCompletableFuture();
    }

    @Override
    public CompletableFuture<Boolean> invalidateAll() {
        return map.deleteAsync()
                    .toCompletableFuture();
    }

    @Override
    public String getName() {
        return map.getName();
    }

    @Override
    public RMap<Object, Object> getNativeCache() {
        return map;
    }
}
