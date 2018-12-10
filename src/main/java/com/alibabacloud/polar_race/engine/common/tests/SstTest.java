package com.alibabacloud.polar_race.engine.common.tests;

import com.alibabacloud.polar_race.engine.common.Constants;
import com.alibabacloud.polar_race.engine.common.common.BloomFilter;
import com.alibabacloud.polar_race.engine.common.index.lsm.Table;
import com.alibabacloud.polar_race.engine.common.index.lsm.TableBuilder;
import com.alibabacloud.polar_race.engine.common.index.map.ConcurrentMapIndexer;
import com.alibabacloud.polar_race.engine.common.util.Convert;
import com.alibabacloud.polar_race.engine.common.util.IOUtils;
import com.alibabacloud.polar_race.engine.common.util.Utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @since 0.1
 * create at 2018/10/25
 */
public class SstTest {

    private static long getBlockAddress(int block) {
        return Table.KV_OFFSET + block * Constants.BLOCK_SIZE;
    }

    private static void readBlock(String fp, long address) {
        byte[] buf = new byte[Constants.BLOCK_SIZE];
        int count = Constants.BLOCK_INDEX_COUNT;
        final int entrySize = Constants.INDEX_ENTRY_SIZE;
        ConcurrentSkipListMap<Long, Long> map = new ConcurrentSkipListMap<>();
        try (RandomAccessFile raf = new RandomAccessFile(fp, "r")) {
            raf.seek(address);
            raf.read(buf);
            for (int i = 0; i < count; i++) {
                final int offset = i * entrySize;
                long k = Convert.bytes2long(buf, offset);
                long v = Convert.bytes2long(buf, offset + 8);
                map.put(k, v);
                System.out.println(String.format("end %d %d", k, v));
            }
            System.out.println(map.get(1000026L));
        } catch (IOException e) {
            Utils.rethrow(e);
        }
    }

    private static void test() {
        final String fp = "E:\\Projects\\Java\\jpolar\\logs\\index\\1.sst";

        try (RandomAccessFile raf = new RandomAccessFile(fp, "r")) {
            int bfSize = TableBuilder.INDEX_COUNT / 8;
            byte[] bfData = new byte[bfSize];
            int read = raf.read(bfData);
            if (read != bfSize) {
                throw new RuntimeException(String.format("need %d  but get %d", bfSize, read));
            }
            BloomFilter bf = new BloomFilter(bfData);
            final int blockCount = TableBuilder.BLOCK_COUNT;
            byte[] buffer = new byte[TableBuilder.BLOCK_MAX_KEY_SIZE];
            long[] blocks = new long[blockCount];
            raf.read(buffer);
            for (int i = 0; i < blockCount; i++) {
                blocks[i] = Convert.bytes2long(buffer, i * 8);
                long blockAddress = getBlockAddress(i);
                raf.seek(blockAddress);
                long k = raf.readLong();
                long v = raf.readLong();
                System.out.println(String.format("start %d %d %d", k, v, blocks[i]));
                k = raf.readLong();
                v = raf.readLong();
                System.out.println(String.format("start %d %d %d", k, v, blocks[i]));
            }

//            for (int i = 0; i < TableBuilder.INDEX_COUNT; i++) {
//                long k = raf.readLong();
//                long v = raf.readLong();
//                System.out.println(String.format("read %d %d %b", k, v, bf.contains(Convert.long2bytes(k))));
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void findInTable(long target) {
        String path = "/data/kv/index/1.sst";
        final Table table = new Table(path);
        System.out.println(table.get(target));
        table.close();
    }

    private static void find(String path, long target) {
        log("find in path %s", path);

        final File dir = new File(path);
        final String[] ssts = dir.list((file, name) -> name.endsWith(".sst"));
        if (ssts == null || ssts.length == 0) {
            return;
        }
        Arrays.sort(ssts, (s1, s2) -> {
            int f1 = Integer.parseInt(IOUtils.getFileName(s1));
            int f2 = Integer.parseInt(IOUtils.getFileName(s2));
            return Integer.compare(f1, f2);
        });
        log("files %d", ssts.length);
        Set<Long> set = new HashSet<>();
        for (String sst : ssts) {
            long address = Table.KV_OFFSET;
            try (RandomAccessFile raf = new RandomAccessFile(String.join(File.separator, path, sst), "r")) {
                raf.seek(Table.KV_OFFSET);
                for (int i = 0; i < TableBuilder.INDEX_COUNT; i++) {
                    final long k = raf.readLong();
                    final long v = raf.readLong();
                    if (set.contains(k)) {
                        log("repeat %s %d % d", sst, i, k);
                    } else {
                        set.add(k);
                    }
//                    if (k >= 1600000) {
//                        log("fuck %s %d % d", sst, i, k);
//                    }
                    if (target == k) {
                        log("find %s %d %d %d %d %d", sst, i, i / Constants.BLOCK_INDEX_COUNT, address, k, v);
                        return;
                    }
                    address += 16;
//                    log("kv %d=%d", k, v);
                }
                log("%s ", sst);
            } catch (IOException e) {
                e.printStackTrace();
                log("err %s", e.getMessage());
            }
        }
    }

    private static void readBlockMaxKey(String fp) {
        long[] blocks = new long[TableBuilder.BLOCK_COUNT];
        try (RandomAccessFile raf = new RandomAccessFile(fp, "r")) {
            raf.seek(TableBuilder.BLOOM_FILTER_SIZE);
            for (int i = 0; i < TableBuilder.BLOCK_COUNT; i++) {
                final long max = raf.readLong();
                blocks[i] = max;
                if (max >= 1099997) {
                    log("block %d %d", i, max);
                }
            }
        } catch (IOException e) {
            Utils.rethrow(e);
        }
        System.out.println(Utils.search(blocks, 1099997));
    }

    private static void findKeyInKeyFile(long key) {
        try (RandomAccessFile raf = new RandomAccessFile("/data/kv/key.data", "r")) {
            byte[] target = Convert.long2bytes(key);
            byte[] bk = new byte[8];
            long length = raf.length();
            long count = length / 8;
            for (long i = 0; i < count; i++) {
                int read = raf.read(bk);
                if (read < 8) {
                    break;
                }
                if (Arrays.equals(bk, target)) {
                    log("find key %d %d %d", key, i, i * 4096);
                    findValueInAddress((i + 1) * 4096);
                    return;
                }
            }
            log("not found %d", key);
        } catch (FileNotFoundException e) {
            log(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void findKeyInAddress(long address) {
        try (RandomAccessFile raf = new RandomAccessFile("/data/kv/key.data", "r")) {
            byte[] bk = new byte[8];
            raf.seek(address);
            raf.read(bk);
            log("key %d", Convert.bytes2long(bk));
        } catch (FileNotFoundException e) {
            log(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void findValueInAddress(long address) {
        try (RandomAccessFile raf = new RandomAccessFile("/data/kv/value.data", "r")) {
            raf.seek(address);
            byte[] value = new byte[8];
            raf.read(value);
            log("find value %d", Convert.bytes2long(value));
        } catch (FileNotFoundException e) {
            log(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void findIndex(long key) {
        ConcurrentMapIndexer indexer = new ConcurrentMapIndexer();
        indexer.open("/data/kv");
        long address = indexer.get(Convert.long2bytes(key));
        log("index %d %d", key, address);
    }


    private long getBlock(long address) {
        address -= Table.KV_OFFSET;
        return address / Constants.BLOCK_SIZE;
    }

    private static void log(String template, Object... args) {
        System.out.println(String.format(template, args));
    }

    public static void main(String[] args) {
        // find 7.sst 11579 45 226224 797907 797907
        long target = -18006482559046977L;
//        find("/data/kv/index", target);
//        findInTable(target);
//        findKeyInKeyFile(580010);
//        findIndex(580010);
//        findKeyInAddress(0);
//        findKeyInKeyFile(6000);
        findKeyInAddress(0);
        findValueInAddress(0);
        findKeyInAddress(8);
        findValueInAddress(4096);
        findKeyInAddress(8 * 2);
        findValueInAddress(4096 * 2);
//        readBlock("E:\\Projects\\Java\\jpolar\\logs\\index\\7.sst", 3928064);
//        readBlock("/tmp/kv/index/1.sst", Table.KV_OFFSET + 242 * TableBuilder.BLOCK_SIZE);
//        readBlock("/tmp/kv/index/7.sst", getBlockAddress(45));
//        readBlockMaxKey("/tmp/kv/index/7.sst");
//        System.out.println(getBlockAddress(45));
//        System.out.println(Table.KV_OFFSET);
//        System.out.println(TableBuilder.BLOCK_SIZE);
//        System.out.println(TableBuilder.BLOCK_COUNT);
//        System.out.println((1036096 - Table.KV_OFFSET));
//        System.out.println((1032192 - Table.KV_OFFSET));
//        System.out.println((1036096 - Table.KV_OFFSET) / TableBuilder.BLOCK_SIZE);
//        System.out.println((1032192 - Table.KV_OFFSET) / TableBuilder.BLOCK_SIZE);
    }
}
