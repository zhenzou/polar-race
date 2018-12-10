package com.alibabacloud.polar_race.engine.common.impl.test;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;

/**
 * 用来测试index性能
 * write直接返回key值
 *
 * @since 0.1
 * create at 2018/10/10
 */
public class TestStorage extends AbstractEngine {
    @Override
    public void open(String path) {

    }

    @Override
    public void close() {

    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        //
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        return key;
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        throw new EngineException(RetCodeEnum.NOT_SUPPORTED, "RANGE TODO");
    }
}
