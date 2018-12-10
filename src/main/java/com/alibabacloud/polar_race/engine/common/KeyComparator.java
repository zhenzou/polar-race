package com.alibabacloud.polar_race.engine.common;

import java.util.Comparator;

/**
 * @since 0.1
 * create at 2018/11/24
 */
@SuppressWarnings("AlibabaClassMustHaveAuthor")
public class KeyComparator implements Comparator<byte[]> {

    @Override
    public int compare(byte[] key1, byte[] key2) {
        for (int i = 0; i < Constants.KEY_SIZE; i++) {
            int a = (key1[i] & 0xff);
            int b = (key2[i] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return 0;
    }
}
