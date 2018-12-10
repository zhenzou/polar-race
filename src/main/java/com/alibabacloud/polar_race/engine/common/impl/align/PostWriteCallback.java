package com.alibabacloud.polar_race.engine.common.impl.align;

import com.alibabacloud.polar_race.engine.common.EntryList;

/**
 * 数据落盘后的回调
 *
 * @since 0.1
 * create at 2018/11/21
 */
public interface PostWriteCallback {

    /**
     * @param entries entries
     */
    default void onBatchWrote(EntryList entries) {

    }

    /**
     * @param keys  keys
     * @param count count
     */
    default void onBatchWrote(byte[][] keys, int count) {

    }
}
