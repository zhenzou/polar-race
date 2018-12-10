package com.alibabacloud.polar_race.engine.common.index;

import com.alibabacloud.polar_race.engine.common.pool.PoolableFactory;

/**
 * @since 0.1
 * create at 2018/10/29
 */
public class IndexFactory implements PoolableFactory<Index> {
    @Override
    public Index create() {
        return new Index();
    }
}
