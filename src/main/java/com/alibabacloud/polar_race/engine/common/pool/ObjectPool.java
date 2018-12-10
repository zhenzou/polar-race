package com.alibabacloud.polar_race.engine.common.pool;

/**
 * @since 0.1
 * create at 2018/11/1
 */
public interface ObjectPool<T extends Poolable> {

    T get();

    void put(T object);
}
