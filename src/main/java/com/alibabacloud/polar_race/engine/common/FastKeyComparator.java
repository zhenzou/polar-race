package com.alibabacloud.polar_race.engine.common;

import java.util.Comparator;

import static com.alibabacloud.polar_race.engine.common.util.UnsafeUtils.ARRAY_BYTE_BASE_OFFSET;
import static com.alibabacloud.polar_race.engine.common.util.UnsafeUtils.UNSAFE;

/**
 * @since 0.1
 * create at 2018/11/24
 */
@SuppressWarnings("AlibabaClassMustHaveAuthor")
public class FastKeyComparator implements Comparator<byte[]> {

    @Override
    public int compare(byte[] key1, byte[] key2) {
        long lw = UNSAFE.getLong(key1, ARRAY_BYTE_BASE_OFFSET);
        long rw = UNSAFE.getLong(key2, ARRAY_BYTE_BASE_OFFSET);
        int n = Long.numberOfTrailingZeros(lw ^ rw) & ~0x7;
        return ((int) ((lw >>> n) & 0xFF)) - ((int) ((rw >>> n) & 0xFF));
    }
}
