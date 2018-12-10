package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.pool.Poolable;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * @since 0.1
 * create at 2018/10/1
 */
public class Entry implements Poolable {

    public byte[] key;

    public byte[] value;

    private final Sync sync;

    public long offset = -1;

    public Entry() {
        this(new byte[Constants.KEY_SIZE], new byte[Constants.VALUE_SIZE]);
    }

    public Entry(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
        sync = new Sync();
    }

    public Entry(byte[] key, byte[] value, Sync sync) {
        this.key = key;
        this.value = value;
        this.sync = sync;
    }

    public void done() {
        sync.releaseShared(1);
    }

    public void await() throws InterruptedException {
        sync.acquireSharedInterruptibly(1);
    }

    @Override
    public void reset() {
        sync.reset();
        offset = -1;
    }

    private static final class Sync extends AbstractQueuedSynchronizer {
        Sync() {
            setState(1);
        }

        void reset() {
            setState(1);
        }

        protected int tryAcquireShared(int acquires) {
            return (getState() == 0) ? 1 : -1;
        }

        protected boolean tryReleaseShared(int releases) {
            for (; ; ) {
                int c = getState();
                if (c == 0) {
                    return false;
                }
                int nextc = c - 1;
                if (compareAndSetState(c, nextc)) {
                    return nextc == 0;
                }
            }
        }
    }

}
