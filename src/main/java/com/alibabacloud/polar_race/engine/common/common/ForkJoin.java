package com.alibabacloud.polar_race.engine.common.common;

import java.util.Objects;

/**
 * @since 0.1
 * create at 2018-12-03
 */
public class ForkJoin<T> {

    private final int concurrency;
    private final Handler<T> handler;

    public ForkJoin(int concurrency, Handler<T> handler) {
        Objects.requireNonNull(handler);
        this.concurrency = concurrency;
        this.handler = handler;
    }

    @SuppressWarnings("AlibabaAvoidManuallyCreateThread")
    public void exec(T object) throws InterruptedException {
        Thread[] ts = new Thread[concurrency];
        for (int i = 0; i < concurrency; i++) {
            final int index = i;
            ts[i] = new Thread(() -> handler.handle(object, concurrency, index));
        }
        for (Thread t : ts) {
            t.start();
        }
        for (Thread t : ts) {
            t.join();
        }
    }

    @FunctionalInterface
    public interface Handler<T> {
        /**
         * @param object 参数
         * @param i      当前线程索引
         */
        void handle(T object, int concurrency, int i);
    }
}
