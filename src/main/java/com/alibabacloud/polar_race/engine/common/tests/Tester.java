package com.alibabacloud.polar_race.engine.common.tests;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.util.Convert;
import com.alibabacloud.polar_race.engine.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @since 0.1
 * create at 2018/9/25
 */
public class Tester {

    private Logger logger = LoggerFactory.getLogger(Tester.class);

    private String impl;
    private String path;
    private int threadNum;
    private int valueNum;

    private int total;

    private AbstractEngine engine;

    public Tester() {
        initConfig();
    }


    /**
     * 从环境变量中获取配置值
     */
    private void initConfig() {
        impl = Utils.getEnvString("tester_engine_impl", "com.alibabacloud.polar_race.engine.common.EngineRace");
        path = Utils.getEnvString("tester_engine_path", ".");
        threadNum = Utils.getEnvInt("tester_engine_thread", 64);
        valueNum = Utils.getEnvInt("tester_value_num", 10_000);
        total = threadNum * valueNum;

        logger.info("engine {}", impl);
        logger.info("thread {}", threadNum);
        logger.info("total  {}", total);
    }

    private void initEngine() {
        try {
            engine = Utils.newInstance(impl);
            engine.open(path);
        } catch (Exception e) {
            Utils.rethrow(e);
        }
    }

    private void runWrite() {
        try {
            initEngine();

            Thread[] threads = new Thread[threadNum];
            for (int i = 0; i < threadNum; i++) {
                Thread t = new WriteThread(i * valueNum, i * valueNum + valueNum);
                threads[i] = t;
                t.start();
            }

            for (Thread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            Utils.rethrow(e);
        } finally {
            engine.close();
        }
    }


    private void runRead() {
        try {
            initEngine();
            Thread[] threads = new Thread[threadNum];
            for (int i = 0; i < threadNum; i++) {
                Thread t = new ReadThread(i * valueNum, i * valueNum + valueNum);
                threads[i] = t;
                t.start();
            }

            for (Thread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            Utils.rethrow(e);
        } finally {
            engine.close();
        }
    }

    private void runRange() {
        try {
            initEngine();
            Thread[] threads = new Thread[threadNum];
            for (int i = 0; i < threadNum; i++) {
                Thread t = new RangeThread(0, total);
                threads[i] = t;
                t.start();
            }

            for (Thread thread : threads) {
                thread.join();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            engine.close();
        }
    }

    public void run() {
        System.out.println("tester start to run");
        long writeStart = System.currentTimeMillis();

        System.out.println("tester start to run write");
        runWrite();
        long writeEnd = System.currentTimeMillis();
        float t1 = (writeEnd - writeStart) / 1000.0f;
        System.out.println("write:" + t1 + " qps:" + (total / t1));

        System.out.println("tester start to run read");
        runRead();
        long readEnd = System.currentTimeMillis();
        float t2 = (readEnd - writeEnd) / 1000.0f;
        System.out.println("read:" + t2 + " qps:" + (total / t2));

        System.out.println("tester start to run range1");
        runRange();
        long range1End = System.currentTimeMillis();
        float t3 = (range1End - readEnd) / 1000.0f;
        System.out.println("range:" + t3 + " qps:" + (total / t3));

        System.out.println("tester start to run range2");
        runRange();
        long range2End = System.currentTimeMillis();
        float t4 = (range2End - range1End) / 1000.0f;
        System.out.println("range:" + t4 + " qps:" + (total / t4));

    }

    public static void main(String[] args) {
        Tester tester = new Tester();
        tester.run();
    }


    byte[] newValue(byte[] key) {
        byte[] value = new byte[4096];
        System.arraycopy(key, 0, value, 0, key.length);
        return value;
    }


    boolean match(byte[] key, byte[] value) {
        for (int i = 0; i < key.length; i++) {
            if (key[i] != value[i]) {
                return false;
            }
        }
        return true;
    }

    class WriteThread extends Thread {

        private final long start;
        private final long end;

        WriteThread(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            final long start = this.start;
            final long end = this.end;
            for (long i = start; i < end; i++) {
                byte[] key = Convert.longToBytes(i);
                try {
                    engine.write(key, newValue(key));
                } catch (EngineException e) {
                    Utils.rethrow(e);
                }
            }
        }
    }

    class ReadThread extends Thread {

        private final long start;
        private final long end;

        ReadThread(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            for (long i = start; i < end; i++) {
                byte[] key = Convert.longToBytes(i);

                try {
                    byte[] read = engine.read(key);
                    byte[] value = new byte[key.length];
                    System.arraycopy(read, 0, value, 0, key.length);
                    long v = Convert.bytesToLong(value);
                    if (v != i) {
                        logger.error("want {} get {} ", i, v);
                        System.exit(-1);
                    }
                } catch (EngineException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }


    /**
     * Range test
     */
    class RangeThread extends Thread {

        private final long start;
        private final long end;

        private final AtomicInteger counter = new AtomicInteger();

        RangeThread(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            byte[] lower = Convert.long2bytes(start);
            byte[] upper = Convert.long2bytes(end);

            try {
                engine.range(null, null, new AbstractVisitor() {
                    @Override
                    public void visit(byte[] key, byte[] value) {
                        if (!match(key, value)) {
                            logger.error("want {} get {} ", Convert.bytes2long(key), Convert.bytes2long(value));
                            System.exit(-1);
                        }
                        counter.incrementAndGet();
                    }
                });
                logger.info("range {}", counter.get());
            } catch (EngineException e) {
                e.printStackTrace();
            }
        }

    }
}
