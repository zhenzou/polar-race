package com.alibabacloud.polar_race.engine.common.common;

import com.alibabacloud.polar_race.engine.common.util.UnsafeUtils;
import sun.misc.Unsafe;
import sun.nio.ch.FileChannelImpl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("restriction")
public class MMapper {

    private static final Method mmap;
    private static final Method unmmap;

    private long address;
    private long size;
    private long length;
    private AtomicLong pos = new AtomicLong();
    private RandomAccessFile raf;
    private FileChannel fc;

    static {
        try {
            mmap = getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
            unmmap = getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Bundle reflection calls to get access to the given method
     *
     * @param clazz  class
     * @param name   name
     * @param params params
     * @return
     * @throws Exception
     */
    private static Method getMethod(Class<?> clazz, String name, Class<?>... params) throws Exception {
        Method m = clazz.getDeclaredMethod(name, params);
        m.setAccessible(true);
        return m;
    }

    /**
     * Round to next 4096 bytes
     *
     * @param i i
     * @return
     */
    private static long roundTo4096(long i) {
        return (i + 0xfffL) & ~0xfffL;
    }

    public MMapper(final String loc, long len) {
        this(new File(loc), len);
    }

    public MMapper(final File file, long len) {
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        this.size = roundTo4096(len);
        this.length = size;
        mapAndSetOffset();
    }

    /**
     * Given that the location and size have been set, map that location
     * for the given length and set this.address to the returned offset
     */
    private void mapAndSetOffset() {
        try {
            raf.setLength(length);
            this.address = (Long) mmap.invoke(fc, 1, length - size, size);
        } catch (IllegalAccessException | InvocationTargetException | IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void close() {
        try {
            unmmap.invoke(fc, address, this.size);
            raf.close();
        } catch (IllegalAccessException | InvocationTargetException | IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * 滚动
     * TODO 自动滚动
     */
    public void roll() {
        try {
            unmmap.invoke(fc, address, this.size);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        pos.set(0);
        length += size;
        mapAndSetOffset();
    }

    public int getInt(long pos) {
        return UnsafeUtils.UNSAFE.getInt(pos + address);
    }

    public long getLong(long pos) {
        return UnsafeUtils.UNSAFE.getLong(pos + address);
    }

    public void putInt(long pos, int val) {
        UnsafeUtils.UNSAFE.putInt(pos + address, val);
    }

    public void putLong(long pos, long val) {
        UnsafeUtils.UNSAFE.putLong(pos + address, val);
    }

    public void putLong(long val) {
        long off = pos.getAndAdd(8);
        UnsafeUtils.UNSAFE.putLong(off + address, val);
    }

    public void put(byte[] data) {
        long off = pos.getAndAdd(data.length);
        UnsafeUtils.UNSAFE.copyMemory(data, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, off + address, data.length);
    }

    /**
     * May want to have offset & length within data as well, for both of these
     */
    public void getBytes(long pos, byte[] data) {
        UnsafeUtils.UNSAFE.copyMemory(null, pos + address, data, Unsafe.ARRAY_BYTE_BASE_OFFSET, data.length);
    }

    public void setBytes(long pos, byte[] data) {
        UnsafeUtils.UNSAFE.copyMemory(data, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, pos + address, data.length);
    }
}
