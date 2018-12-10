package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.util.Convert;

import java.util.Random;

/**
 * @since 0.1
 * create at 2018/12/4
 */
public class LongKeyComparator {
    public int compare(long l1, long l2) {
        long xor = l1 ^ l2;
        if (xor >= 0) {
            return Long.compare(l1, l2);
        } else {
            return -Long.compare(l1, l2);
        }
    }

    public static void test(long l1, long l2) {
        KeyComparator c1 = new KeyComparator();
        LongKeyComparator c2 = new LongKeyComparator();
        byte[] b1 = Convert.long2bytes(l1);
        byte[] b2 = Convert.long2bytes(l2);
        int compare1 = c1.compare(b1, b2);
        if (compare1 > 0) {
            compare1 = 1;
        }
        if (compare1 < 0) {
            compare1 = -1;
        }
        int compare2 = c2.compare(l1, l2);
        if (compare1 != compare2) {
            System.out.printf("compare %d %d  expect %d get %d\n", l1, l2, compare1, compare2);
        }
    }

    public static void main(String[] args) {
        Random random = new Random();
        for (int i = 0; i < 10000; i++) {
            long l1 = random.nextLong();
            long l2 = random.nextLong();
            test(l1, l2);
        }
        test(-1, 0);
        test(-1, -1);
        test(1, 0);
        test(1, -1);
    }
}
