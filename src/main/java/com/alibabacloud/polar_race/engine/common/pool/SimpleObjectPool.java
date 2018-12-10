package com.alibabacloud.polar_race.engine.common.pool;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * @since 0.1
 * create at 2018/11/1
 */
public class SimpleObjectPool<T extends Poolable> implements ObjectPool<T> {

    private Queue<T> queue;

    private PoolableFactory<T> factory;

    public SimpleObjectPool(int cap, PoolableFactory<T> factory) {
        this.factory = factory;
        queue = new ArrayDeque<>(cap);
        for (int i = 0; i < cap; i++) {
            queue.offer(factory.create());
        }
    }

    @Override
    public T get() {
        final T object = queue.poll();
        if (object == null) {
            return factory.create();
        }
        return object;
    }

    @Override
    public void put(T object) {
        if (object != null) {
            object.reset();
            queue.offer(object);
        }
    }

}
