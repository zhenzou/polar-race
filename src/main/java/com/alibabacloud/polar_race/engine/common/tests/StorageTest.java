package com.alibabacloud.polar_race.engine.common.tests;

import com.alibabacloud.polar_race.engine.common.Constants;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @since 0.1
 * create at 2018/10/27
 */
public class StorageTest {

    private static void checkKeys(Set<Long> keys, String fp) {

        final Set<Long> storagedKeyss = new HashSet<>(keys.size());
        System.out.println("keys:" + keys.size());


        try (RandomAccessFile raf = new RandomAccessFile(fp, "r")) {
            final long length = raf.length();
            System.out.println("length:" + length);
            System.out.println(length % Constants.VALUE_LOG_ENTRY_SIZE);
            for (long i = 0; i < length / Constants.VALUE_LOG_ENTRY_SIZE; i++) {
                final long key = raf.readLong();
                storagedKeyss.add(key);
                raf.skipBytes(4096);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("storagedKeyss:" + storagedKeyss.size());
        for (Long key : keys) {
            if (!storagedKeyss.contains(key)) {
                System.out.println("error " + key);
            }
        }
    }

    private static Set<Long> loadKeys(String fp) {
        List<String> strings = new ArrayList<>();
        try {
            strings = Files.readAllLines(Paths.get(fp));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return strings.stream().map(Long::valueOf).collect(Collectors.toSet());
    }

    private static void findKey(String fp, long target) {
        try (RandomAccessFile raf = new RandomAccessFile(fp, "r")) {
            final long length = raf.length();
            System.out.println("length:" + length);
            System.out.println(length % Constants.VALUE_LOG_ENTRY_SIZE);
            for (long i = 0; i < length / Constants.VALUE_LOG_ENTRY_SIZE; i++) {
                final long key = raf.readLong();
                if (target == key) {
                    System.out.println(String.format("found %d %d", key, i));
                    return;
                }
                raf.skipBytes(4096);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("not found " + target);
    }

    public static void main(String[] args) {
//        final Set<Long> keys = loadKeys("/tmp/kv/keys.txt");
//        checkKeys(keys, "/tmp/kv/value.data");
        findKey("/data/kv/value.data", 40000);
    }
}
