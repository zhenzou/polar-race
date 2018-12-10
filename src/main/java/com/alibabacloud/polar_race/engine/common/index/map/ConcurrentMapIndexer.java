package com.alibabacloud.polar_race.engine.common.index.map;

import com.alibabacloud.polar_race.engine.common.Constants;
import com.alibabacloud.polar_race.engine.common.index.Indexer;
import com.alibabacloud.polar_race.engine.common.util.Convert;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.alibabacloud.polar_race.engine.common.util.Utils.log;

/**
 * @since 0.1
 * create at 2018/10/1
 */
public class ConcurrentMapIndexer implements Indexer {

    private static final int BATCH_SIZE = 32 * 1024 * 1024;

    private static final int BATCH = BATCH_SIZE / Constants.INDEX_ENTRY_SIZE;

    private LongLongHashMap indexes = new LongLongHashMap(4000000, 0.99);

    private File file;

    private long checkPoint;

    @Override
    public void open(String path) {
        path = String.join(File.separator, path, Constants.INDEX_FILE_NAME);
        file = new File(path);
        if (file.exists()) {
            load();
        } else {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void load() {
        log("map index start to load");
        try (FileInputStream in = new FileInputStream(file)) {
            FileChannel fc = in.getChannel();
            ByteBuffer buffer = ByteBuffer.allocateDirect(BATCH_SIZE);
            long checkPoint = this.checkPoint;
            while (true) {
                buffer.clear();
                final int read = fc.read(buffer);
                final int count = read / Constants.INDEX_ENTRY_SIZE;
                buffer.flip();
                for (int i = 0; i < count; i++) {
                    final long key = buffer.getLong();
                    final long value = buffer.getLong();
                    indexes.put(key, value);
                    if (value > checkPoint) {
                        checkPoint = value;
                    }
                }
                if (read < BATCH_SIZE) {
                    break;
                }
            }
            this.checkPoint = checkPoint;
        } catch (EOFException e) {
            log(e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log("map index load %d", indexes.size());
    }

    @Override
    public synchronized void set(byte[] key, long address) {
        indexes.put(Convert.bytes2long(key), address);
    }

    @Override
    public long get(byte[] key) {
        return indexes.getOrDefault(Convert.bytes2long(key), -1);
    }

    @Override
    public long getCheckPoint() {
        return checkPoint;
    }


    private void write(FileChannel fc, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            fc.write(buffer);
        }
    }

    @Override
    public void close() {
        log("map index start to close");
        try (FileOutputStream out = new FileOutputStream(file)) {
            FileChannel fc = out.getChannel();
            ByteBuffer buffer = ByteBuffer.allocateDirect(BATCH_SIZE);
            int count = 0;
            for (LongLongCursor cursor : indexes) {
                buffer.putLong(cursor.key);
                buffer.putLong(cursor.value);
                count++;
                if (count == BATCH) {
                    buffer.flip();
                    write(fc, buffer);
                    buffer.clear();
                    count = 0;
                }
            }
            if (count != 0) {
                buffer.flip();
                write(fc, buffer);
            }
            log("write %d %d", indexes.size(), count);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log("map index close");
    }
}
