package com.alibabacloud.polar_race.engine.common.pool;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;

import java.util.concurrent.BlockingQueue;

/**
 * @since 0.1
 * create at 2018/11/1
 */
public class ConcurrentObjectPool<T extends Poolable> implements ObjectPool<T> {

    private BlockingQueue<T> queue;

    private PoolableFactory<T> factory;

    public ConcurrentObjectPool(int cap, PoolableFactory<T> factory) {
        this.factory = factory;
        queue = new DisruptorBlockingQueue<>(cap);
        for (int i = 0; i < cap; i++) {
            queue.offer(factory.create());
        }
    }

    @Override
    public T get() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            return factory.create();
        }
    }

    @Override
    public void put(T object) {
        if (object != null) {
            object.reset();
            queue.offer(object);
        }
    }
}
