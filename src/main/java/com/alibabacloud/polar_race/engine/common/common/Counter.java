package com.alibabacloud.polar_race.engine.common.common;

/**
 * @since 0.1
 * create at 2018/12/5
 */
public class Counter {
    private int count;

    public void inc() {
        count++;
    }

    public int count() {
        return count;
    }
}
