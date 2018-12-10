package com.alibabacloud.polar_race.engine.common.impl.align;

import com.alibabacloud.polar_race.engine.common.Constants;
import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.common.BasicThreadFactory;
import com.alibabacloud.polar_race.engine.common.common.DirectIoLib;
import com.alibabacloud.polar_race.engine.common.Entry;
import com.alibabacloud.polar_race.engine.common.EntryList;
import com.alibabacloud.polar_race.engine.common.util.UnsafeUtils;
import com.alibabacloud.polar_race.engine.common.util.Utils;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibabacloud.polar_race.engine.common.util.Utils.log;

/**
 * @since 0.1
 * create at 2018/10/1
 */
public class DirectWriter extends Thread {

    private static final int WAITING = 1;
    private static final int WRITING = 2;
    private static final int TO_CLOSE = 3;
    private static final int CLEANED = 4;
    private static final int CLOSED = 5;

    private final BlockingQueue<Entry> queue;

    private final AtomicInteger state = new AtomicInteger(WAITING);

    private ThreadPoolExecutor executor =
            new ThreadPoolExecutor(1, 1, 24,
                    TimeUnit.HOURS, new LinkedBlockingDeque<>(32),
                    new BasicThreadFactory.Builder()
                            .namingPattern("[ENGINE-WRITER]-%d")
                            .daemon(true)
                            .build());

    private final byte[][] keys = new byte[128][];

    private int fd;

    private PointerByReference ref = new PointerByReference();

    private long offset;

    private PostWriteCallback callback;

    public DirectWriter(String path, BlockingQueue<Entry> queue) {
        this(new File(String.join(File.separator, path, Constants.VALUE_FILE_NAME)),
                null,
                queue);
    }

    public DirectWriter(File valueFile, PostWriteCallback callback, BlockingQueue<Entry> queue) {
        this.queue = queue;
        this.callback = callback;
        try {
            DirectIoLib.posix_memalign(ref, EngineRace.ALIGNMENT, new NativeLong(128 * Constants.VALUE_SIZE));
            this.fd = DirectIoLib.openFileDirect(valueFile.getAbsolutePath(), false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param entries entries
     * @return length
     */
    private long encode(EntryList entries) {
        Pointer share = ref.getValue().share(0);
        long offset = UnsafeUtils.getPointerPeer(share);
        long count = 0;
        for (int i = 0; i < entries.size(); i++) {
            Entry entry = entries.get(i);
            keys[i] = entry.key;
            byte[] value = entry.value;
            UnsafeUtils.UNSAFE.copyMemory(value, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, offset + count, value.length);
            count += value.length;
        }
        return count;
    }

    private void doWrite(EntryList entries) {
        try {
            long length = encode(entries);
            DirectIoLib.pwrite(fd, ref.getValue(), new NativeLong(length), new NativeLong(offset));
            callback.onBatchWrote(keys, entries.size());
            offset += length;
        } finally {
            for (Entry entry : entries) {
                entry.done();
            }
            state.set(WAITING);
        }
    }

    private EntryList write(EntryList entries) {
        if (entries.isEmpty()) {
            return entries;
        }
        state.set(WRITING);
        executor.execute(() -> this.doWrite(entries));
        return new EntryList(8);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public void close() {
        state.set(TO_CLOSE);
        while (!state.compareAndSet(CLEANED, CLOSED)) {
            // spin to closed
        }
        log("writer closed");
    }

    private void clean() {
        try {
            Utils.shutdown(executor);
        } catch (Exception e) {
            Utils.rethrow(e);
        } finally {
            state.set(CLEANED);
        }
    }

    @Override
    public void run() {
        EntryList entries = new EntryList(8);
        try {
            while (true) {
                Entry entry = null;
                entry = queue.poll(500, TimeUnit.NANOSECONDS);
                if (entry != null) {
                    entries.add(entry);
                } else if (state.get() == TO_CLOSE) {
                    write(entries);
                    clean();
                    return;
                }
                if (state.get() == WAITING) {
                    entries = write(entries);
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
