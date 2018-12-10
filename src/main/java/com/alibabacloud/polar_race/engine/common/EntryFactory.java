package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.pool.PoolableFactory;

/**
 * @since 0.1
 * create at 2018/10/29
 */
public class EntryFactory implements PoolableFactory<Entry> {

    @Override
    public Entry create() {
        return new Entry();
    }
}
