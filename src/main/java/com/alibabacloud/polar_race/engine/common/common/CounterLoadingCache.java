package com.alibabacloud.polar_race.engine.common.common;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @since 0.1
 * create at 2018-12-06
 */
public class CounterLoadingCache<K, V> {

    private final int max;
    private final ConcurrentMap<K, Holder<V>> holders;
    private final Function<K, Holder<V>> loader;

    public CounterLoadingCache(int max, int cap, Function<K, V> loader) {
        Objects.requireNonNull(loader);
        this.max = max;
        this.holders = new ConcurrentHashMap<>(cap);
        this.loader = k -> new Holder<>(loader.apply(k));
    }

    public V get(K key) {
        Holder<V> holder = holders.computeIfAbsent(key, loader);
        int count = holder.count.addAndGet(1);
        if (count == max) {
            holders.remove(key);
        }
        return holder.value;
    }

    private static class Holder<V> {
        final V value;
        final AtomicInteger count = new AtomicInteger();

        private Holder(V value) {
            this.value = value;
        }
    }
}
