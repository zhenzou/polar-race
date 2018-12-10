package com.alibabacloud.polar_race.engine.common;

import static com.alibabacloud.polar_race.engine.common.util.Utils.log;

/**
 * @since 0.1
 * create at 2018/12/6
 */
public class Timer {
    private final String name;
    private final long ts;

    private Timer(String name) {
        this.name = name;
        this.ts = System.currentTimeMillis();
    }

    public static Timer start(String name) {
        log("timer-%s start", name);
        return new Timer(name);
    }

    public void end() {
        log("start-%s cost %d", name, System.currentTimeMillis() - ts);
    }
}
