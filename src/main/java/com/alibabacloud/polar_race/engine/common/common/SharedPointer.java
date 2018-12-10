package com.alibabacloud.polar_race.engine.common.common;

import com.alibabacloud.polar_race.engine.common.Constants;
import com.alibabacloud.polar_race.engine.common.pool.ObjectPool;
import com.alibabacloud.polar_race.engine.common.pool.Poolable;
import com.alibabacloud.polar_race.engine.common.pool.PoolableFactory;
import com.alibabacloud.polar_race.engine.common.pool.SimpleObjectPool;
import com.alibabacloud.polar_race.engine.common.util.IOUtils;
import com.sun.jna.ptr.PointerByReference;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @since 0.1
 * create at 2018-12-10
 */
public class SharedPointer implements Poolable {

    public static final ObjectPool<SharedPointer> pool = new SimpleObjectPool<>(16, new SharedPointerFactory(512 * Constants.VALUE_SIZE));

    public static SharedPointer get() {
        return pool.get();
    }

    public static void put(SharedPointer p) {
        int i = p.count.addAndGet(1);
        if (i == 64) {
            pool.put(p);
        }
    }

    public final PointerByReference ref;

    private AtomicInteger count = new AtomicInteger();

    public SharedPointer(long size) {
        ref = IOUtils.newPointer(size);
    }

    public SharedPointer(PointerByReference ref) {
        this.ref = ref;
    }

    public void free() {
        int i = count.addAndGet(1);
        if (i == 64) {
            DirectIoLib.free(ref.getValue());
        }
    }

    @Override
    public void reset() {

    }

    public static class SharedPointerFactory implements PoolableFactory<SharedPointer> {

        private final long size;

        public SharedPointerFactory(long size) {
            this.size = size;
        }

        @Override
        public SharedPointer create() {
            return new SharedPointer(size);
        }
    }
}
