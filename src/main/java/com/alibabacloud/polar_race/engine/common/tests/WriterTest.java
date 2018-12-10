package com.alibabacloud.polar_race.engine.common.tests;

import com.alibabacloud.polar_race.engine.common.util.Utils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 */
public class WriterTest {

    private static int valueSize;
    private static int count;
    private static String path;

    public static void main(String[] args) throws IOException {
        initConfig();
        long start = System.currentTimeMillis();
//        writeByFc();
        writeByOutputStream();
//        writeByDirect();
        long end = System.currentTimeMillis();
        System.out.println("cost -> " + (end - start));
    }

    private static void writeByFc() throws IOException {
        try (FileOutputStream fos = new FileOutputStream(path)) {
            FileChannel fc = fos.getChannel();

            for (int i = 0; i < count; i++) {
                fc.write(ByteBuffer.wrap(newValue(i)));
            }
        }
    }

    private static void writeByOutputStream() throws IOException {
        try (FileOutputStream fos = new FileOutputStream(path)) {
            for (int i = 0; i < count; i++) {
                fos.write(newValue(i));
            }
        }
    }

    static byte[] newValue(int key) {
        byte[] value = new byte[valueSize];
        value[key % valueSize] = 1;
        return value;
    }

    private static void initConfig() {
        valueSize = Utils.getEnvInt("tester_value_size", 4 * 1024);
        count = Utils.getEnvInt("tester_count", 250000);
        path = Utils.getEnvString("test_path", "/data/test.log");
    }
}
