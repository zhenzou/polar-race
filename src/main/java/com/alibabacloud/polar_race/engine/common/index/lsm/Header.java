package com.alibabacloud.polar_race.engine.common.index.lsm;

import com.alibabacloud.polar_race.engine.common.Constants;
import com.alibabacloud.polar_race.engine.common.util.Convert;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.SortedMap;

/**
 * @since 0.1
 * create at 2018/11/5
 */
class Header {
    long minKey;
    long maxKey;
    int indexCount;
    int blockCount;

    static Header fromRaf(RandomAccessFile raf) {
        Header header = new Header();
        try {
            header.minKey = raf.readLong();
            header.maxKey = raf.readLong();
            header.indexCount = raf.readInt();
            header.blockCount = raf.readInt();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return header;
    }

    static Header fromTable(SortedMap<Long, Long> table) {
        final Header header = new Header();
        header.minKey = table.firstKey();
        header.maxKey = table.lastKey();
        header.indexCount = table.size();
        header.blockCount = (int) Math.ceil(((double) table.size()) / Constants.BLOCK_INDEX_COUNT);

        return header;
    }

    void encode(byte[] table) {
        Convert.long2bytes(minKey, table, 0);
        Convert.long2bytes(maxKey, table, 8);
        Convert.int2bytes(indexCount, table, 16);
        Convert.int2bytes(blockCount, table, 20);
    }

    @Override
    public String toString() {
        return "Header{" +
                "minKey=" + minKey +
                ", maxKey=" + maxKey +
                ", indexCount=" + indexCount +
                ", blockCount=" + blockCount +
                '}';
    }
}
