package com.alibabacloud.polar_race.engine.common.index.lsm;

import com.alibabacloud.polar_race.engine.common.Constants;
import com.alibabacloud.polar_race.engine.common.common.BloomFilter;
import com.alibabacloud.polar_race.engine.common.util.Convert;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.alibabacloud.polar_race.engine.common.util.Utils.*;

/**
 * @since 0.1
 * create at 2018/10/22
 */
public class Table {

    private Header header;

    final String fp;

    private BloomFilter bf;

    public static final long KV_OFFSET = TableBuilder.METADATA_SIZE;

    private long[] blocks;

    private RandomAccessFile raf;

    private FileChannel fc;

    private ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.BLOCK_SIZE);

    private LoadingCache<Long, LongLongMap> cache = Caffeine.newBuilder()
            .maximumSize(8)
            .recordStats()
            .build(this::read);

    public Table(String fp) {
        this.fp = fp;
        init();
    }

    /**
     * 初始化bf等
     */
    private void init() {
        try {
            raf = new RandomAccessFile(fp, "r");
            fc = raf.getChannel();
            header = Header.fromRaf(raf);
            initBloomFilter();
            initBlocks();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void initBloomFilter() throws IOException {
        byte[] data = new byte[TableBuilder.BLOOM_FILTER_SIZE];
        int read = raf.read(data);
        if (read != TableBuilder.BLOOM_FILTER_SIZE) {
            throw new RuntimeException(String.format("need %d  but get %d", Constants.BLOCK_SIZE, read));
        }
        bf = new BloomFilter(data);
    }

    private void initBlocks() throws IOException {
        final int count = header.blockCount;
        long[] blocks = new long[count];
        byte[] bytes = new byte[TableBuilder.BLOCK_MAX_KEY_SIZE];
        raf.read(bytes);
        for (int i = 0; i < count; i++) {
            final long key = Convert.bytes2long(bytes, i * Constants.KEY_SIZE);
            blocks[i] = key;
        }
        this.blocks = blocks;
    }

    public boolean contains(long key) {
        return key >= header.minKey && key <= header.maxKey && contains(Convert.long2bytes(key));
    }

    public boolean contains(byte[] bk, long key) {
        return key >= header.minKey && key <= header.maxKey && contains(bk);
    }

    public boolean contains(byte[] key) {
        return bf.contains(key);
    }

    /**
     * @param address offset of sst to read
     * @return block index
     */
    private synchronized LongLongMap read(long address) {
        LongLongMap indexes = new LongLongHashMap(Constants.BLOCK_INDEX_COUNT);
        try {
            fc.read(buffer, address);
            buffer.flip();
            for (int i = 0; i < Constants.BLOCK_INDEX_COUNT; i++) {
                long key = buffer.getLong();
                long value = buffer.getLong();
                indexes.put(key, value);
            }
            buffer.clear();
        } catch (IOException e) {
            rethrow(e);
        }
        return indexes;
    }

    /**
     * @param raf     raf
     * @param address offset of sst to read
     * @return block index
     */
    private LongLongMap read(RandomAccessFile raf, long address) {
        LongLongMap indexes = new LongLongHashMap(Constants.BLOCK_INDEX_COUNT);
        byte[] buf = new byte[Constants.BLOCK_SIZE];
        try {
            raf.seek(address);
            raf.read(buf);
            for (int i = 0; i < Constants.BLOCK_INDEX_COUNT; i++) {
                final int offset = i * Constants.INDEX_ENTRY_SIZE;
                long key = Convert.bytes2long(buf, offset);
                long value = Convert.bytes2long(buf, offset + 8);
                indexes.put(key, value);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return indexes;
    }

    private long getBlockAddress(int block) {
        return KV_OFFSET + block * Constants.BLOCK_SIZE;
    }

    /**
     * 获取key对应的address
     * 调用这个方法表示，key很有可能在这个sst里面
     *
     * @param key key
     * @return address
     */
    public long get(long key) {
        int block = search(blocks, key);
        long address = getBlockAddress(block);
        LongLongMap indexes = cache.get(address);
        if (indexes == null) {
            log("read null %d %d", key, address);
            return -1;
        }
        return indexes.getOrDefault(key, -1);
    }

    public void close() {
        CacheStats stats = cache.stats();
        log("cache stats %s %d %d %d %.4f %d %.4f", fp, stats.requestCount(), stats.evictionCount(),
                stats.hitCount(), stats.hitRate(), stats.missCount(), stats.missRate());
        try {
            raf.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
