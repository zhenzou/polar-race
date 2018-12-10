package com.alibabacloud.polar_race.engine.common.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.System.out;

public interface Utils {

    boolean DEBUG = true;

    static boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    static void assertNotEmpty(String str) {
        assertTrue(!isEmpty(str));
    }

    static void assertTrue(boolean flag) {
        if (!flag) {
            throw new RuntimeException();
        }
    }

    static void assertNot(boolean flag) {
        if (flag) {
            throw new RuntimeException();
        }
    }

    static void rethrow(Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
    }

    static String getEnvString(String key, String def) {
        return getEnv(key).orElse(def);
    }

    static int getEnvInt(String key, int def) {
        return getEnv(key).map(Integer::parseInt).orElse(def);
    }

    static Optional<String> getEnv(String key) {
        return Optional.ofNullable(System.getenv(key));
    }

    static <T> T newInstance(String klass) {
        T instance = null;
        try {
            // 暂时的优化，因为索引没有落盘
            @SuppressWarnings("unchecked")
            Class<T> clazz = (Class<T>) Class.forName(klass);
            instance = (T) clazz.newInstance();
        } catch (
                Exception e) {
            Utils.rethrow(e);
        }
        return instance;
    }

    static <K, V> String printMap(Map<K, V> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            sb.append(String.format("%s => %s \n", entry.getKey().toString(), entry.getValue().toString()));
        }
        return sb.toString();
    }

    /**
     * search the index of target or smallest value which larger than target in values
     *
     * @param values values
     * @param target target
     * @return index
     */
    static int search(long[] values, long target) {

        int i = 0;
        int j = values.length;
        while (i < j) {
            int mid = (i + j) >> 1;
            final long midValue = values[mid];
            if (target > midValue) {
                i = mid + 1;
            } else if (target < midValue) {
                j = mid;
            } else {
                return mid;
            }
        }
        return j;
    }

    /**
     * search the index of target or smallest value which larger than target in values
     *
     * @param values     values
     * @param target     target
     * @param comparator comparator
     * @param <T>        T
     * @return index
     */
    static <T> int search(T[] values, T target, Comparator<T> comparator) {
        int i = 0;
        int j = values.length;
        while (i < j) {
            int mid = (i + j) >> 1;
            final T midValue = values[mid];
            int compare = comparator.compare(target, midValue);
            if (compare > 0) {
                i = mid + 1;
            } else if (compare < 0) {
                j = mid;
            } else {
                return mid;
            }
        }
        return j;
    }

    static void sleep(int mills) {
        try {
            Thread.sleep(mills);
        } catch (InterruptedException e) {
            Utils.rethrow(e);
        }
    }

    @SuppressWarnings("while")
    static void shutdown(ExecutorService executor) {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                // spin to close
            }
        } catch (InterruptedException e) {
            Utils.rethrow(e);
        }
    }

    static byte[] align(int count) {
        int align = count % 4096;
        return new byte[align];
    }

    static int compare(byte[] bytes1, int offset1, int length1,
                       byte[] bytes2, int offset2, int length2) {
        if (bytes1 == bytes2 &&
                offset1 == offset2 &&
                length1 == length2) {
            return 0;
        }
        int end1 = offset1 + length1;
        int end2 = offset2 + length2;
        for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
            int a = (bytes1[i] & 0xff);
            int b = (bytes2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return length1 - length2;
    }

    DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy:MM:dd HH:mm:ss");

    static void log(String fmt, Object... args) {
        if (!DEBUG) {
            return;
        }
        LocalDateTime now = Instant.now().atZone(ZoneId.systemDefault()).toLocalDateTime();
        if (args.length > 0) {
            out.println(Thread.currentThread().getName() + " " + DATE_TIME_FORMATTER.format(now) + " " + String.format(fmt, args));
        } else {
            out.println(Thread.currentThread().getName() + " " + DATE_TIME_FORMATTER.format(now) + " " + fmt);
        }
    }

    static void assertEquals(int expect, int actual) {
        if (expect != actual) {
            out.println(String.format("failed expect %d actual %d", expect, actual));
        }
    }

    static void main(String[] args) {
        long[] values = new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
        assertEquals(4, search(values, 5));
        assertEquals(2, search(values, 3));
        assertEquals(19, search(values, 200));
    }
}
