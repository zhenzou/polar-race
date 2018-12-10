package com.alibabacloud.polar_race.engine.common;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @since 0.1
 * create at 2018/12/3
 */
public class Stats {
    private static final ConcurrentMap<String, Stats> stats = new ConcurrentHashMap<>();

    public static Stats start(String name) {
        Stats stats = Stats.stats.computeIfAbsent(name, Stats::new);
        stats.start();
        return stats;
    }

    public static Stats start(String first, String... others) {
        return start(first + "-" + String.join("-", others));
    }

    public static void display() {
        String str = stats
                .entrySet()
                .stream()
                .sorted(Comparator.comparingLong(o -> o.getValue().ts))
                .reduce("", (s, e) -> s + "\n" + e.getValue().toString(), (s, s2) -> s2);
        System.out.println("Stats:" + str);
    }

    private final long ts;

    private final String name;

    private final List<Record> records = new ArrayList<>();

    public Stats(String name) {
        this.name = name;
        ts = System.currentTimeMillis();
    }

    public void record(String name) {
        Record prev = null;
        if (!records.isEmpty()) {
            prev = records.get(records.size() - 1);
        }
        records.add(new Record(name, System.currentTimeMillis(), prev));
    }

    public void start() {
        record("start");
    }

    public void end() {
        record("end");
    }

    private static class Record {
        final String name;
        final long ts;
        final Record prev;

        private Record(String name, long ts, Record prev) {
            this.name = name;
            this.ts = ts;
            this.prev = prev;
        }

        @Override
        public String toString() {
            if (prev == null) {
                return "";
            } else {
                return String.format("[ %s-%s %d ] ", prev.name, name, ts - prev.ts);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(name + ":");
        for (Record record : records) {
            sb.append(record.toString());
        }
        return sb.toString();
    }

    public static void main(String[] args) throws InterruptedException {
        Stats stats = Stats.start("test");
        Thread.sleep(1000);
        stats.record("mid");
        stats.record("end");
        Stats.display();
    }
}
