package com.alibabacloud.polar_race.engine.common.impl.align;

import com.alibabacloud.polar_race.engine.common.*;
import com.alibabacloud.polar_race.engine.common.common.*;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.index.Index;
import com.alibabacloud.polar_race.engine.common.util.Convert;
import com.alibabacloud.polar_race.engine.common.util.IOUtils;
import com.alibabacloud.polar_race.engine.common.util.UnsafeUtils;
import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;
import com.koloboke.collect.map.hash.HashLongLongMap;
import com.koloboke.function.LongLongConsumer;
import com.sun.jna.NativeLong;
import com.sun.jna.ptr.PointerByReference;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibabacloud.polar_race.engine.common.Constants.*;
import static com.alibabacloud.polar_race.engine.common.common.DirectIoLib.openFileDirect;
import static com.alibabacloud.polar_race.engine.common.common.DirectIoLib.pread;
import static com.alibabacloud.polar_race.engine.common.util.Utils.log;

@SuppressWarnings("FieldCanBeLocal")
public class Engine implements PostWriteCallback {

    private static final AtomicInteger RANGE_COUNTER = new AtomicInteger();

    private static final ThreadLocal<byte[]> THREAD_LOCAL_BYTES = new ThreadLocal<>();
    private static final ThreadLocal<Entry> THREAD_LOCAL_ENTRIES = new ThreadLocal<>();
    private static final ThreadLocal<PointerByReference> THREAD_LOCAL_REFS = new ThreadLocal<>();
    private static final EngineException NOT_FOUND = new EngineException(RetCodeEnum.NOT_FOUND, "not found key");

    private static final int CONCURRENCY = 64;

    private static final int CACHE_BATCH = 512;

    private static final List<BlockingQueue<SharedPointer>> BUFFER = new ArrayList<>(CONCURRENCY);

    private static final Executor SHARED_EXECUTOR = new ThreadPoolExecutor(64, 64, 1L, TimeUnit.DAYS,
            new LimitedBlockingQueue<>(CACHE_BATCH),
            new BasicThreadFactory.Builder().clazz(ReaderThread.class).daemon(true).namingPattern("RANGE_READER-%d").build());

    private String path;

    private AlignedMapIndexer indexer;

    private DirectWriter writer;

    private BlockingQueue<Entry> queue;

    private int fd = -1;

    private String valueFilePath;

    private String keyFilePath;

    private File valueFile;

    private File keyFile;

    private Index[] sortedIndexes;

    static {
        for (int i = 0; i < CONCURRENCY; i++) {
            BUFFER.add(new DisruptorBlockingQueue<>(16));
        }
    }

    private CheckedOnce<Void, EngineException> once = new CheckedOnce<>(this::initRange);

    public void open(String path) throws EngineException {
        this.path = path;
        IOUtils.createDir(path);
        init();
        log("engine opened %s", path);
    }

    private void init() throws EngineException {
        try {
            valueFilePath = String.join(File.separator, path, Constants.VALUE_FILE_NAME);
            valueFile = new File(valueFilePath);
            if (!valueFile.exists()) {
                valueFile.createNewFile();
            }
            keyFilePath = String.join(File.separator, path, Constants.KEY_FILE_NAME);
            keyFile = new File(keyFilePath);
            if (!keyFile.exists()) {
                //在这里不创建keyFile，indexer会创建 写阶段才需要,trick
                queue = new DisruptorBlockingQueue<>(Constants.QUEUE_SIZE, SpinPolicy.BLOCKING);
                writer = new DirectWriter(valueFile, this, queue);
                writer.start();
            }

            fd = openFileDirect(valueFilePath, true);
            indexer = new AlignedMapIndexer(valueFile.length());
            indexer.open(path);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, e.getMessage());
        }
    }

    public void write(byte[] key, byte[] value) throws EngineException {
        Entry entry = THREAD_LOCAL_ENTRIES.get();
        if (entry == null) {
            entry = new Entry();
            THREAD_LOCAL_ENTRIES.set(entry);
        }
        try {
            entry.key = key;
            entry.value = value;
            queue.put(entry);
            entry.await();
        } catch (InterruptedException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, e.getMessage());
        } finally {
            entry.reset();
        }
    }

    public byte[] read(byte[] key) throws EngineException {
        final long address = indexer.get(key);
        if (address < 0) {
            throw NOT_FOUND;
        }
        byte[] value = THREAD_LOCAL_BYTES.get();
        if (value == null) {
            value = new byte[VALUE_SIZE];
            THREAD_LOCAL_BYTES.set(value);
        }
        read(address, value);
        return value;
    }

    @SuppressWarnings("squid:S2259")
    private void read(long address, byte[] value) {
        PointerByReference ref = THREAD_LOCAL_REFS.get();
        if (ref == null) {
            ref = IOUtils.newPointer();
            THREAD_LOCAL_REFS.set(ref);
        }
        pread(fd, ref.getValue().share(0), NATIVE_VALUE_SIZE, new NativeLong(address));
        long offset = UnsafeUtils.getPointerPeer(ref.getValue());
        UnsafeUtils.UNSAFE.copyMemory(null, offset, value, Unsafe.ARRAY_BYTE_BASE_OFFSET, VALUE_SIZE);
    }

    public void sort() throws EngineException {
        if (valueFile.length() <= 0 || sortedIndexes != null) {
            return;
        }
        Stats stats = Stats.start(path, "sort");
        indexer.load(null);
        // 先排序index
        HashLongLongMap indexes = indexer.indexes;
        if (indexes == null || indexes.isEmpty()) {
            return;
        }
        stats.record("index");
        sortedIndexes = new Index[indexes.size()];
        log("%s sort start %d", path, valueFile.length());

        Counter counter = new Counter();
        indexes.forEach((LongLongConsumer) (a, b) -> {
            sortedIndexes[counter.count()] = new Index(a, b);
            counter.inc();
        });

        stats.record("assign");
        // 删除，节省内存
        indexes = null;
        indexer.indexes = null;
        DualPivotQuickSort.sort(sortedIndexes, Comparator.naturalOrder());
        stats.end();
    }

    /**
     * search the index of target or smallest value which larger than target in values
     *
     * @param target target
     * @return index
     */
    private int search(byte[] target) {
        Comparator<byte[]> comparator = EngineRace.KEY_COMPARATOR;
        int i = 0;
        int j = sortedIndexes.length;
        byte[] midValue = new byte[KEY_SIZE];
        Index[] block = this.sortedIndexes;
        while (i < j) {
            int mid = (i + j) >> 1;
            Convert.long2bytes(block[mid].key, midValue, 0);
            int compare = comparator.compare(target, midValue);
            if (compare > 0) {
                i = mid + 1;
            } else if (compare < 0) {
                j = mid;
            } else {
                return mid;
            }
        }
        return j;
    }

    private int getMin(byte[] lower) {
        int min = 0;
        if (lower != null) {
            min = search(lower);
        }
        return min;
    }

    private int getMax(byte[] upper) {
        int max = sortedIndexes.length;
        if (upper != null) {
            max = search(upper);
        }
        return max;
    }

    @SuppressWarnings("AlibabaAvoidManuallyCreateThread")
    private void initRange(Void arg) throws EngineException {
        sort();
        Thread t = new Thread(() -> read());
        t.start();
    }

    private void read() {

        Stats stats = Stats.start(path, "range", "read");

        Index[] sortedIndexes = this.sortedIndexes;
        int indexCount = sortedIndexes.length;
        int batchCount = indexCount / CACHE_BATCH;

        log("read range %s %d %d %d", path, indexCount, batchCount, CACHE_BATCH);

        try {
            for (int i = 0; i < batchCount; i++) {
                CountDownLatch latch = new CountDownLatch(CACHE_BATCH);
                SharedPointer p = SharedPointer.get();
                int offset = i * CACHE_BATCH;
                for (int j = 0; j < CACHE_BATCH; j++) {
                    Index index = sortedIndexes[offset + j];
                    SHARED_EXECUTOR.execute(new ReadTask(fd, j, index.address, p, latch));
                }
                latch.await();
                for (BlockingQueue<SharedPointer> queue : BUFFER) {
                    queue.put(p);
                }
            }
            int left = indexCount % CACHE_BATCH;
            if (left != 0) {
                SharedPointer p = SharedPointer.get();
                int offset = batchCount * CACHE_BATCH;
                CountDownLatch latch = new CountDownLatch(left);
                for (int j = 0; j < left; j++) {
                    Index index = sortedIndexes[offset + j];
                    SHARED_EXECUTOR.execute(new ReadTask(fd, j, index.address, p, latch));
                }
                latch.await();
                for (BlockingQueue<SharedPointer> queue : BUFFER) {
                    queue.put(p);
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            once.reset();
        }
        stats.end();
        this.sortedIndexes = null;
    }

    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        if (!valueFile.exists() || valueFile.length() == 0) {
            return;
        }
        int id = RANGE_COUNTER.getAndIncrement();
        Stats stats = Stats.start(path, "range", id + "");

        once.exec(null);

        int min = getMin(lower);
        int max = getMax(upper);

        Index[] sortedIndexes = this.sortedIndexes;
        int batchCount = sortedIndexes.length / CACHE_BATCH;

        BlockingQueue<SharedPointer> quque = BUFFER.get(id % CONCURRENCY);

        byte[] key = new byte[KEY_SIZE];
        byte[] value = new byte[VALUE_SIZE];
        try {
            for (int i = 0; i < batchCount; i++) {
                int offset = i * CACHE_BATCH;
                SharedPointer p = quque.take();
                for (int j = 0; j < CACHE_BATCH; j++) {
                    Convert.long2bytes(sortedIndexes[offset + j].key, key, 0);
                    p.ref.getValue().read(j * VALUE_SIZE, value, 0, VALUE_SIZE);
                    visitor.visit(key, value);
                }
                SharedPointer.put(p);
            }
            int left = sortedIndexes.length % CACHE_BATCH;
            if (left != 0) {
                int offset = batchCount * CACHE_BATCH;
                SharedPointer p = quque.take();
                for (int j = 0; j < left; j++) {
                    p.ref.getValue().read(j * VALUE_SIZE, value, 0, VALUE_SIZE);
                    Convert.long2bytes(sortedIndexes[offset + j].key, key, 0);
                    visitor.visit(key, value);
                }
                SharedPointer.put(p);
            }
        } catch (InterruptedException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, e.getMessage());
        }
        stats.end();
    }

    @Override
    public void onBatchWrote(byte[][] keys, int count) {
        indexer.add(keys, count);
    }

    @Override
    public void onBatchWrote(EntryList entries) {
        for (Entry entry : entries) {
            indexer.add(entry.key);
        }
    }

    public void close() {
        if (fd >= 0) {
            DirectIoLib.close(fd);
        }
        if (writer != null) {
            writer.close();
        }
        if (indexer != null) {
            indexer.close();
        }
        log("engine closed %s", path);
    }

    static class ReadTask implements Runnable {

        private final int fd;
        private final int index;
        private final long address;
        private final SharedPointer p;
        private final CountDownLatch latch;

        ReadTask(int fd, int index, long address, SharedPointer p, CountDownLatch latch) {
            this.fd = fd;
            this.index = index;
            this.address = address;
            this.p = p;
            this.latch = latch;
        }

        @Override
        public void run() {
            pread(fd, p.ref.getValue().share(index * VALUE_SIZE), NATIVE_VALUE_SIZE, new NativeLong(address));
            latch.countDown();
        }
    }
}
