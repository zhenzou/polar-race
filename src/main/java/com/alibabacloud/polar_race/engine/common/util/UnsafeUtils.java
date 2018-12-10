package com.alibabacloud.polar_race.engine.common.util;

import com.sun.jna.Pointer;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * @since 0.1
 * create at 2018/11/15
 */
public class UnsafeUtils {

    public static final sun.misc.Unsafe UNSAFE;
    public static final long POINTER_PEER_OFFSET;
    public static final long ARRAY_BYTE_BASE_OFFSET;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);

            Field pf = Pointer.class.getDeclaredField("peer");
            POINTER_PEER_OFFSET = UNSAFE.objectFieldOffset(pf);
            ARRAY_BYTE_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static long getPointerPeer(Pointer pointer) {
        return UNSAFE.getLong(pointer, POINTER_PEER_OFFSET);
    }
}
