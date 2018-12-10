package com.alibabacloud.polar_race.engine.common.common;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @since 0.1
 * create at 2018/10/27
 */
public class LimitedBlockingQueue<E> extends LinkedBlockingQueue<E> {

    public LimitedBlockingQueue(int capacity) {
        super(capacity);
    }

    @Override
    public boolean offer(E e) {
        // turn offer() and add() into a blocking calls (unless interrupted)
        try {
            put(e);
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return false;
    }
}

