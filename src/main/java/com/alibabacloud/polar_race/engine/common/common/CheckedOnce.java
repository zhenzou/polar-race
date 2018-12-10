package com.alibabacloud.polar_race.engine.common.common;

import java.util.Objects;

/**
 * @since 0.1
 * create at 2018-12-02
 */
public class CheckedOnce<T, E extends Exception> {

    private volatile boolean done;

    private Function<T, E> f;

    public CheckedOnce(Function<T, E> f) {
        Objects.requireNonNull(f);
        this.f = f;
    }

    public void exec(T arg) throws E {
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

    public void reset() {
        done = false;
    }


    @FunctionalInterface
    public interface Function<T, E extends Exception> {
        void apply(T arg) throws E;
    }
}
