package com.alibabacloud.polar_race.engine.common.common;

import com.alibabacloud.polar_race.engine.common.util.Hash;

import java.util.BitSet;

/**
 * @since 0.1
 * create at 2018/10/17
 */
public class BloomFilter {

    private Hasher[] hashers = new Hasher[]{Hash::BKDRHash, Hash::JSHash, Hash::DJBHash};

    private BitSet bitSet;

    private int bitSize;

    public BloomFilter(int bitSize) {
        this.bitSize = bitSize;
        bitSet = new BitSet(bitSize);
    }

    public BloomFilter(byte[] bytes) {
        bitSet = BitSet.valueOf(bytes);
        bitSize = bytes.length * 8;
    }

    public void mark(byte[] key) {
        final int bitSize = this.bitSize;
        for (Hasher hasher : hashers) {
            final int hash = hasher.hash(key) % bitSize;
            bitSet.set(hash);
        }
    }

    public boolean contains(byte[] key) {
        final int bitSize = this.bitSize;
        for (Hasher hasher : hashers) {
            final int hash = hasher.hash(key) % bitSize;
            if (!bitSet.get(hash)) {
                return false;
            }
        }
        return true;
    }

    public void clear() {
        bitSet.clear();
    }

    public byte[] encode() {
        return bitSet.toByteArray();
    }


    @FunctionalInterface
    public interface Hasher {
        /**
         * hash
         *
         * @param key key
         * @return hash
         */
        int hash(byte[] key);
    }
}
