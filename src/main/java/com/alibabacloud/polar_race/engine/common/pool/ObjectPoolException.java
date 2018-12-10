package com.alibabacloud.polar_race.engine.common.pool;

/**
 * @since 0.1
 * create at 2018/11/3
 */
public class ObjectPoolException extends RuntimeException {
    ObjectPoolException(String message, Throwable cause) {
        super(message, cause);
    }
}
