package com.alibabacloud.polar_race.engine.common.index.lsm;

import com.alibabacloud.polar_race.engine.common.Constants;
import com.alibabacloud.polar_race.engine.common.common.BloomFilter;
import com.alibabacloud.polar_race.engine.common.common.Tuple;
import com.alibabacloud.polar_race.engine.common.util.Convert;
import com.alibabacloud.polar_race.engine.common.util.Utils;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.alibabacloud.polar_race.engine.common.Constants.KEY_SIZE;
import static java.lang.System.arraycopy;

/**
 * @since 0.1
 * create at 2018/10/17
 */
public class TableBuilder {

    /**
     * bloom filter 占用内存大小
     */
    public static final int INDEX_COUNT = LSMIndexer.SSTABLE_SIZE / Constants.INDEX_ENTRY_SIZE;

    /**
     * 每个sst中的block数量
     */
    public static final int BLOCK_COUNT = INDEX_COUNT / Constants.BLOCK_INDEX_COUNT;

    /**
     * max key ,min key index count
     */
    public static final int HEADER_SIZE = 24;

    /**
     * bloom filter 占内存大小
     */
    public static final int BLOOM_FILTER_SIZE = INDEX_COUNT / 8;

    /**
     * sst 中记录每个block最大key的区域大小
     */
    public static final int BLOCK_MAX_KEY_SIZE = BLOCK_COUNT * 8;

    /**
     * block max key 在文件中的偏移量
     */
    public static final int BLOCK_MAX_KEY_OFFSET = HEADER_SIZE + BLOOM_FILTER_SIZE;

    /**
     * 所有metadata加起来的大小
     */
    public static final int METADATA_SIZE = HEADER_SIZE + BLOOM_FILTER_SIZE + BLOCK_MAX_KEY_SIZE;

    public static final int TOTAL_TABLE_SIZE = LSMIndexer.SSTABLE_SIZE + METADATA_SIZE;

    private static final byte[] BUFFER = new byte[TOTAL_TABLE_SIZE];

    static Tuple<byte[], Long> build(ConcurrentSkipListMap<Long, Long> table) {

        Header header = Header.fromTable(table);
        byte[] buffer = BUFFER;
        header.encode(buffer);

        int offset = METADATA_SIZE;
        int keys = 0;
        int blocks = 0;
        long maxAddress = 0;

        BloomFilter bf = new BloomFilter(INDEX_COUNT);
        byte[] kb = new byte[KEY_SIZE];
        for (Map.Entry<Long, Long> entry : table.entrySet()) {
            final long key = entry.getKey();
            final long address = entry.getValue();
            if (address > maxAddress) {
                maxAddress = address;
            }

            kb = Convert.long2bytes(key);
            bf.mark(kb);
            arraycopy(kb, 0, buffer, offset, KEY_SIZE);
            Convert.long2bytes(address, buffer, offset + KEY_SIZE);

            keys++;
            offset += Constants.INDEX_ENTRY_SIZE;

            if (keys == Constants.BLOCK_INDEX_COUNT) {
                arraycopy(kb, 0, buffer, BLOCK_MAX_KEY_OFFSET + blocks * KEY_SIZE, KEY_SIZE);
                blocks++;
                keys = 0;
            }
        }
        // 有重复的key,最后的block不一定会满
        if (keys != 0) {
            arraycopy(kb, 0, buffer, BLOCK_MAX_KEY_OFFSET + blocks * KEY_SIZE, KEY_SIZE);
            blocks++;
        }
        Utils.log("table %d %d %d %d", blocks, keys, table.size(), offset);
        final byte[] bytes = bf.encode();
        arraycopy(bytes, 0, buffer, HEADER_SIZE, bytes.length);

        Arrays.fill(buffer, HEADER_SIZE + bytes.length, BLOCK_MAX_KEY_OFFSET, (byte) 0);
        Arrays.fill(buffer, BLOCK_MAX_KEY_OFFSET + blocks * KEY_SIZE, METADATA_SIZE, (byte) 0);
        Arrays.fill(buffer, offset, TOTAL_TABLE_SIZE, (byte) 0);
        return new Tuple<>(buffer, maxAddress);
    }
}
