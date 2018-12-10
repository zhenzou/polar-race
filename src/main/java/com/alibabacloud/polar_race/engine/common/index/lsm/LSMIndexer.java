package com.alibabacloud.polar_race.engine.common.index.lsm;

import com.alibabacloud.polar_race.engine.common.Constants;
import com.alibabacloud.polar_race.engine.common.common.BasicThreadFactory;
import com.alibabacloud.polar_race.engine.common.common.Tuple;
import com.alibabacloud.polar_race.engine.common.index.Index;
import com.alibabacloud.polar_race.engine.common.index.IndexFactory;
import com.alibabacloud.polar_race.engine.common.index.Indexer;
import com.alibabacloud.polar_race.engine.common.pool.ConcurrentObjectPool;
import com.alibabacloud.polar_race.engine.common.pool.ObjectPool;
import com.alibabacloud.polar_race.engine.common.util.Convert;
import com.alibabacloud.polar_race.engine.common.util.IOUtils;
import com.alibabacloud.polar_race.engine.common.util.Utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import static com.alibabacloud.polar_race.engine.common.util.Utils.log;

/**
 * @since 0.1
 * create at 2018/10/8
 */
public class LSMIndexer implements Indexer {

    private static final int STATE_RUNNING = 1;
    private static final int STATE_TO_CLOSE = 2;
    private static final int STATE_CLOSED = 3;

    private static final String INDEX_DIR = "index";

    private static final String SSTABLE_EXT = ".sst";

    private static final String CHECK_POINT_FILE = "checkpoint";

    /**
     * 64M
     */
    public static final int SSTABLE_SIZE = 32 * 1024 * 1024;

    /**
     * mutable mem table
     */
    private ConcurrentSkipListMap<Long, Long> mt;

    private BlockingQueue<Index> queue;

    private ThreadPoolExecutor executor;

    private List<Table> tables;

    private volatile int state;

    private Flusher flusher;

    private String path;

    private File dir;

    private OutputStream checkpointFile;

    private int nextFileId;

    private long checkpoint;

    private ObjectPool<Index> pool;

    @Override
    public void open(String path) {
        this.path = String.join(File.separator, path, INDEX_DIR);
        log("open lsm index dir %s", this.path);
        executor = new ThreadPoolExecutor(1,
                1,
                24,
                TimeUnit.HOURS,
                new LinkedBlockingDeque<>(32),
                new BasicThreadFactory.Builder().namingPattern("[LSM-FLUSHER]-%d").build());
        mt = new ConcurrentSkipListMap<>();
        queue = new LinkedBlockingQueue<>(1024);
        state = STATE_RUNNING;
        tables = new ArrayList<>();
        checkpoint = 0;
        initDir();
        initTables();
        initNextFileId();
        initFlusher();
        initCheckPoint();
        initPool();
    }

    private void initDir() {
        dir = new File(path);
        if (!dir.exists()) {
            final boolean mkdirs = dir.mkdirs();
            if (!mkdirs) {
                throw new RuntimeException("can not make new index dir " + path);
            }
        }
    }

    private void initPool() {
        pool = new ConcurrentObjectPool<>(4096, new IndexFactory());
    }

    private void initTables() {
        final String[] ssts = dir.list((f, name) -> name.endsWith(SSTABLE_EXT));
        if (ssts == null || ssts.length == 0) {
            return;
        }
        Arrays.sort(ssts, (s1, s2) -> {
            int f1 = Integer.parseInt(IOUtils.getFileName(s1));
            int f2 = Integer.parseInt(IOUtils.getFileName(s2));
            return Integer.compare(f1, f2);
        });
        for (String sst : ssts) {
            final String fp = String.join(File.separator, this.path, sst);
            log("add table %s", fp);
            tables.add(new Table(fp));
        }
    }

    private void initNextFileId() {
        if (!tables.isEmpty()) {
            Table table = tables.get(tables.size() - 1);
            nextFileId = Integer.parseInt(IOUtils.getFileName(table.fp));
        }
        log("file id %d", nextFileId);
    }

    private void initFlusher() {
        flusher = new Flusher(queue);
        flusher.start();
    }

    private void initCheckPoint() {

        final File file = new File(String.join(File.separator, path, CHECK_POINT_FILE));
        try {
            if (!file.exists()) {
                if (!file.createNewFile()) {
                    throw new RuntimeException("can not make file " + file.getPath());
                }
            } else {
                final byte[] bytes = Files.readAllBytes(file.toPath());
                final int length = bytes.length;
                for (int i = 0; i < length; i += 8) {
                    final long cp = Convert.bytes2long(bytes, i);
                    if (cp > checkpoint) {
                        checkpoint = cp;
                    }
                }
            }
            checkpointFile = new FileOutputStream(file, true);
        } catch (IOException e) {
            Utils.rethrow(e);
        }
        log("checkpoint %d", checkpoint);
    }

    @Override
    public void set(byte[] key, long address) {
        try {
            final Index index = pool.get();
            index.key = Convert.bytes2long(key);
            index.address = address;
            queue.put(index);
        } catch (Exception e) {
            log("write error for %s", e.getMessage());
            Utils.rethrow(e);
        }
    }

    @Override
    public long get(byte[] key) {
        final long lk = Convert.bytesToLong(key);
        final long address = getFromMemory(lk);
        if (address >= 0) {
            return address;
        }
        return getFromTable(key, lk);
    }

    private long getFromTable(byte[] bk, long key) {
        final int size = tables.size();
        for (int i = size - 1; i >= 0; i--) {
            final Table table = tables.get(i);
            if (table.contains(bk, key)) {
                long address = table.get(key);
                if (address >= 0) {
                    return address;
                }
            }
        }
        return -1;
    }

    @SuppressWarnings("unchecked")
    private long getFromMemory(long key) {
        Long address = mt.get(key);
        if (address != null) {
            return address;
        }
        return -1;
    }

    private String nextFile() {
        nextFileId++;
        return String.join(File.separator, path, String.valueOf(nextFileId) + SSTABLE_EXT);
    }

    /**
     * 暂时不压缩，LSM和value
     *
     * @param table table
     * @param close
     */
    @SuppressWarnings("squid:S1319")
    private void doFlush(ConcurrentSkipListMap<Long, Long> table, boolean close) {
        if (!table.isEmpty()) {
            final Tuple<byte[], Long> encode = TableBuilder.build(table);
            final String fp = nextFile();
            log("new index file %s", fp);
            final File file = IOUtils.newFile(fp);
            IOUtils.write(file, encode.first);
            updateCheckPoint(encode.second);
        }
        if (close) {
            state = STATE_CLOSED;
        }
    }

    private void updateCheckPoint(long address) {
        try {
            checkpointFile.write(Convert.long2bytes(address));
            checkpointFile.flush();
            checkpoint = address;
        } catch (IOException e) {
            Utils.rethrow(e);
        }
    }

    @Override
    public long getCheckPoint() {
        return checkpoint;
    }

    @Override
    public void close() {
        state = STATE_TO_CLOSE;
        while (state != STATE_CLOSED) {
            Utils.sleep(10);
        }
        Utils.shutdown(executor);
        for (Table table : tables) {
            table.close();
        }
        try {
            checkpointFile.close();
        } catch (IOException e) {
            Utils.rethrow(e);
        }
        log("lsm indexer closed %d %d", mt.size(), queue.size());
    }

    /**
     * @since 0.1
     * create at 2018/10/10
     */
    class Flusher extends Thread {

        private final BlockingQueue<Index> queue;

        Flusher(BlockingQueue<Index> queue) {
            this.queue = queue;
            this.setName("[LSM-FLUSHER-MAIN]");
        }

        @SuppressWarnings("squid:S1319")
        private void flush(boolean close) {
            final ConcurrentSkipListMap<Long, Long> tmp = LSMIndexer.this.mt;
            mt = new ConcurrentSkipListMap<>();
            executor.submit(() -> doFlush(tmp, close));
        }

        @SuppressWarnings("squid:S2142")
        @Override
        public void run() {
            int count = 0;
            while (true) {
                Index index = null;
                try {
                    index = queue.poll(1, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Utils.rethrow(e);
                }
                if (index == null) {
                    if (state == STATE_TO_CLOSE) {
                        flush(true);
                        log("flusher closed");
                        return;
                    }
                    continue;
                }
                if (count >= SSTABLE_SIZE) {
                    count = 0;
                    flush(false);
                }
                Long old = mt.put(index.key, index.address);
                if (old == null) {
                    count += Constants.INDEX_ENTRY_SIZE;
                }
                index.reset();
                pool.put(index);
            }
        }
    }
}
