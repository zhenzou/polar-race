package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.common.CheckedOnce;
import com.alibabacloud.polar_race.engine.common.common.DirectIoLib;
import com.alibabacloud.polar_race.engine.common.common.ForkJoin;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.impl.align.Engine;
import com.alibabacloud.polar_race.engine.common.util.IOUtils;
import com.sun.jna.NativeLong;

import java.io.File;

import static com.alibabacloud.polar_race.engine.common.util.Utils.log;

/**
 * 根据range分区
 *
 * @since 0.1
 * create at 2018/11/1
 */
public class EngineRace extends AbstractEngine {

    /**
     * 暂时用16个分区，便于处理
     */
    private static final int PARTITION = 16;

    public static NativeLong ALIGNMENT = new NativeLong(512);

    public static final KeyComparator KEY_COMPARATOR = new KeyComparator();

    public static final LongKeyComparator LONG_KEY_COMPARATOR = new LongKeyComparator();

    private Engine[] engines;

    private CheckedOnce<Integer, EngineException> sortOnce = new CheckedOnce<>(this::sort);

    /**
     * open Engine
     *
     * @param path the path of engine store data.
     * @throws EngineException
     */
    @SuppressWarnings("AlibabaAvoidManuallyCreateThread")
    @Override
    public void open(String path) throws EngineException {
        IOUtils.createDir(path);
        ALIGNMENT = new NativeLong(DirectIoLib.getBlockSize(path));
        engines = new Engine[PARTITION];
        for (int i = 0; i < PARTITION; i++) {
            final Engine engine = new Engine();
            engine.open(String.join(File.separator, path, "engine-" + i));
            engines[i] = engine;
        }
    }

    /**
     * close Engine
     */
    @Override
    public void close() {
        for (Engine engine : engines) {
            engine.close();
        }
        Stats.display();
    }

    private int partition(byte[] key) {
        return (key[0] & 0xFF) >>> 4;
    }

    /**
     * write a key-value pair into engine
     *
     * @param key   key
     * @param value value
     * @throws EngineException
     */
    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        int index = partition(key);
        engines[index].write(key, value);
    }

    /**
     * read value of a key
     *
     * @param key key
     * @return value value
     * @throws EngineException
     */
    @Override
    public byte[] read(byte[] key) throws EngineException {
        int index = partition(key);
        return engines[index].read(key);
    }

    private void sort(Integer concurrency) throws EngineException {
        ForkJoin<Engine[]> forkJoin = new ForkJoin<>(concurrency, this::sort);
        Stats stats = Stats.start("engine-sort");
        try {
            forkJoin.exec(engines);
        } catch (InterruptedException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, e.getMessage());
        }
        stats.record("end");
    }

    private void sort(Engine[] engines, int concurrency, int start) {
        for (int i = 0; i < engines.length; i++) {
            if (i % concurrency == start) {
                try {
                    engines[i].sort();
                } catch (EngineException e) {
                    e.printStackTrace(System.out);
                }
            }
        }
    }

    /**
     * applies the given AbstractVisitor.Visit() function to the result of every key-value pair in the key range [first, last),
     * in order
     *
     * @param lower   start key
     * @param upper   end key
     * @param visitor is check key-value pair,you just call visitor.visit(String key, String value) function in your own engine.
     * @throws EngineException
     */
    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        int lp = 0;
        int up = PARTITION - 1;
        if (lower != null) {
            lp = partition(lower);
        }
        if (upper != null) {
            up = partition(upper);
        }
        log("partition %d %d", lp, up);
        for (int i = lp; i <= up; i++) {
            Engine engine = engines[i];
            engine.range(lower, upper, visitor);
        }
    }
}
