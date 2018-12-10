package com.alibabacloud.polar_race.engine.common.util;

import java.nio.ByteBuffer;

/**
 * 字节转换的工具方法
 */
public interface Convert {

    static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    static byte[] long2bytes(long v) {
        byte[] b = new byte[8];
        b[7] = (byte) v;
        b[6] = (byte) (v >>> 8);
        b[5] = (byte) (v >>> 16);
        b[4] = (byte) (v >>> 24);
        b[3] = (byte) (v >>> 32);
        b[2] = (byte) (v >>> 40);
        b[1] = (byte) (v >>> 48);
        b[0] = (byte) (v >>> 56);
        return b;
    }

    static void long2bytes(long v, byte[] b, int offset) {
        b[7 + offset] = (byte) v;
        b[6 + offset] = (byte) (v >>> 8);
        b[5 + offset] = (byte) (v >>> 16);
        b[4 + offset] = (byte) (v >>> 24);
        b[3 + offset] = (byte) (v >>> 32);
        b[2 + offset] = (byte) (v >>> 40);
        b[1 + offset] = (byte) (v >>> 48);
        b[offset] = (byte) (v >>> 56);
    }

    static void int2bytes(long v, byte[] b, int offset) {
        b[3 + offset] = (byte) v;
        b[2 + offset] = (byte) (v >>> 8);
        b[1 + offset] = (byte) (v >>> 16);
        b[offset] = (byte) (v >>> 24);
    }

    static long bytes2long(byte[] b) {
        return ((b[7] & 0xFFL)) +
                ((b[6] & 0xFFL) << 8) +
                ((b[5] & 0xFFL) << 16) +
                ((b[4] & 0xFFL) << 24) +
                ((b[3] & 0xFFL) << 32) +
                ((b[2] & 0xFFL) << 40) +
                ((b[1] & 0xFFL) << 48) +
                (((long) b[0]) << 56);
    }

    static long bytes2long(byte[] b, int offset) {
        return ((b[7 + offset] & 0xFFL)) +
                ((b[6 + offset] & 0xFFL) << 8) +
                ((b[5 + offset] & 0xFFL) << 16) +
                ((b[4 + offset] & 0xFFL) << 24) +
                ((b[3 + offset] & 0xFFL) << 32) +
                ((b[2 + offset] & 0xFFL) << 40) +
                ((b[1 + offset] & 0xFFL) << 48) +
                (((long) b[offset]) << 56);
    }

    static long key2Long(byte[] key) {
        return UnsafeUtils.UNSAFE.getLong(key, UnsafeUtils.ARRAY_BYTE_BASE_OFFSET);
    }

    static void bench() {
        long start = System.currentTimeMillis();
        for (long i = 0; i < 100000000; i++) {
            byte[] bytes = long2bytes(i);
//            byte[] bytes = longToBytes(i);
            long l = bytes2long(bytes);
//            long l = key2Long(bytes);
            if (l != i) {
                System.out.println("not match i:" + i + " l:" + l);
            }
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    static void main(String[] args) {
        bench();
    }
}
