package com.alibabacloud.polar_race.engine.common.index;

import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.pool.Poolable;

/**
 * @since 0.1
 * create at 2018/10/31
 */
public class Index implements Poolable, Comparable<Index> {
    public long key;
    public long address;

    public Index(long key, long address) {
        this.key = key;
        this.address = address;
    }

    public Index() {
    }

    @Override
    public void reset() {
    }

    @Override
    public int compareTo(Index o) {
        return EngineRace.LONG_KEY_COMPARATOR.compare(key, o.key);
    }
}
