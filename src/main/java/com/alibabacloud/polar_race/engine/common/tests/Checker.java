package com.alibabacloud.polar_race.engine.common.tests;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.util.Convert;
import com.alibabacloud.polar_race.engine.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 测试一致性
 *
 * @since 0.1
 * create at 2018/9/25
 */
public class Checker {

    private Logger logger = LoggerFactory.getLogger(Checker.class);

    private static final String KEYS_FILE = "keys.txt";

    private String impl;
    private String path;
    private FileOutputStream out;
    private final Object lock = new Object();
    private File keysFile;
    private int threadNum;
    private int valueNum;

    private AbstractEngine engine;

    public Checker() {
        initConfig();
    }

    private File getKeysFile() {
        return new File(String.join(File.separator, path, KEYS_FILE));
    }

    /**
     * 从环境变量中获取配置值
     */
    private void initConfig() {
        impl = Utils.getEnvString("tester_engine_impl", "com.alibabacloud.polar_race.engine.common.EngineRace");
        path = Utils.getEnvString("tester_engine_path", ".");
        threadNum = Utils.getEnvInt("tester_engine_thread", 64);
        valueNum = Utils.getEnvInt("tester_value_num", 10_000);
        int total = threadNum * valueNum;

        logger.info("total :{}", total);
    }

    private void initEngine() {
        try {
            try {
                out = new FileOutputStream(keysFile, true);
            } catch (FileNotFoundException e) {
                Utils.rethrow(e);
            }
            // 暂时的优化，因为索引没有落盘
            engine = Utils.newInstance(impl);
            engine.open(path);
        } catch (Exception e) {
            Utils.rethrow(e);
        }
    }

    private void writeKey(byte[] key) {
        synchronized (lock) {
            try {
                out.write(key);
                out.flush();
            } catch (IOException e) {
                Utils.rethrow(e);
            }
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


    private void runCheck() {
        List<Long> fails = new ArrayList<>();
        try {
            initEngine();
            final byte[] bytes = Files.readAllBytes(keysFile.toPath());
            final int count = bytes.length / 8;
            logger.info("keys:{} {}", count, bytes.length % 8);
            for (int i = 0; i < count; i += 1) {
                final int start = i * 8;
                final byte[] bk = Arrays.copyOfRange(bytes, start, start + 8);
                try {
                    final byte[] read = engine.read(bk);
                    byte[] value = new byte[8];
                    System.arraycopy(read, 0, value, 0, 8);
                    if (!Arrays.equals(bk, value)) {
                        logger.error("want {} get {} ", Convert.bytesToLong(bk), Convert.bytes2long(value));
                    }
                } catch (EngineException e) {
                    fails.add(Convert.bytes2long(bk));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            engine.close();
        }
        logger.info("failed count:{}", fails.size());
        for (Long fail : fails) {
            logger.info("fail:{}", fail);
        }
    }

    public void run() {
        System.out.println("checker start to run");
        keysFile = getKeysFile();
        if (keysFile.exists()) {
            System.out.println("run check");
            runCheck();
        } else {
            System.out.println("run write");
            boolean newFile = false;
            try {
                newFile = keysFile.createNewFile();
            } catch (IOException e) {
                Utils.rethrow(e);
            }
            if (!newFile) {
                throw new RuntimeException("can not make file " + keysFile.getAbsolutePath());
            }
            runWrite();
        }
    }

    public static void main(String[] args) {
        Checker tester = new Checker();
        tester.run();
    }


    private byte[] newValue(byte[] key) {
        byte[] value = new byte[4096];
        System.arraycopy(key, 0, value, 0, key.length);
        return value;
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
                    writeKey(key);
                } catch (EngineException e) {
                    Utils.rethrow(e);
                }
            }
        }
    }
}
