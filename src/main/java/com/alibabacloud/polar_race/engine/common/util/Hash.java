package com.alibabacloud.polar_race.engine.common.util;

/**
 * @since 0.1
 * create at 2018/10/17
 */
public interface Hash {

    /**
     * BKDR Hash Function
     */
    static int BKDRHash(byte[] bytes) {
        // 31 131 1313 13131 131313 etc..
        int seed = 131;
        int hash = 0;
        for (byte b : bytes) {
            hash = hash * seed + b;
        }
        return (hash & 0x7FFFFFFF);
    }

    /**
     * AP Hash Function
     */
    static int APHash(byte[] bytes) {
        int hash = 0;
        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            if ((i & 1) == 0) {
                hash ^= ((hash << 7) ^ b ^ (hash >> 3));
            } else {
                hash ^= (~((hash << 11) ^ b ^ (hash >> 5)));
            }
        }
        return (hash & Integer.MAX_VALUE);
    }

    /**
     * DJB Hash Function
     */
    static int DJBHash(byte[] bytes) {
        int hash = 5381;
        for (byte b : bytes) {
            hash += (hash << 5) + (b);

        }
        return (hash & Integer.MAX_VALUE);
    }

    /**
     * JS Hash Function
     */
    static int JSHash(byte[] bytes) {
        int hash = 1315423911;
        for (byte b : bytes) {
            hash ^= ((hash << 5) + b + (hash >> 2));
        }
        return (hash & Integer.MAX_VALUE);
    }

    public static void main(String[] args) {
        System.out.println(BKDRHash("string".getBytes()));
        System.out.println(APHash("string".getBytes()));
        System.out.println(DJBHash("string".getBytes()));
        System.out.println(JSHash("string".getBytes()));
    }
}
