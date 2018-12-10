package com.alibabacloud.polar_race.engine.common.index;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.util.Iterator;

/**
 * @since 0.1
 * create at 2018/10/8
 */
public interface Indexer {

    /**
     * open
     *
     * @param path path
     */
    default void open(String path) {
    }

    /**
     * sort index
     */
    default void sort() throws EngineException {
        throw new UnsupportedOperationException("by default,sort is not supported");
    }

    /**
     * set value address of key
     *
     * @param key     key
     * @param address address
     */
    void set(byte[] key, long address);

    /**
     * add key to the indexer,the indexer should determinate the offset by itself
     *
     * @param key key
     */
    default void add(byte[] key) {
        throw new UnsupportedOperationException("by default,add is not supported");
    }

    /**
     * add key to the indexer,the indexer should determinate the offset by itself
     *
     * @param keys key
     */
    default void add(byte[][] keys, int count) {
        throw new UnsupportedOperationException("by default,add is not supported");
    }

    /**
     * range
     *
     * @param lower low
     * @param upper upper
     * @return iter range
     */
    default Iterator<Index> iter(byte[] lower, byte[] upper) {
        throw new UnsupportedOperationException("by default,iter is not supported");
    }

    /**
     * get value offset of key
     *
     * @param key key
     * @return offset
     */
    long get(byte[] key);

    /**
     * 获取最近落盘的offset
     * 以便重放valueLog，重建索引
     *
     * @return offset
     */
    default long getCheckPoint() {
        return -1;
    }

    /**
     * close
     */
    default void close() {
    }
}
