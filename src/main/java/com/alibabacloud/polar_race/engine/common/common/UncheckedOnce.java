package com.alibabacloud.polar_race.engine.common.common;

import java.util.Objects;

/**
 * @since 0.1
 * create at 2018-12-02
 */
public class UncheckedOnce<T> {

    private volatile boolean done;

    private Function<T> f;

    public UncheckedOnce(Function<T> f) {
        Objects.requireNonNull(f);
        this.f = f;
    }

    public void exec(T arg) {
        if (done) {
            return;
        }
        synchronized (this) {
            if (done) {
                return;
            }
            f.apply(arg);
            done = true;
        }
    }


    @FunctionalInterface
    public interface Function<T> {
        void apply(T arg);
    }
}
