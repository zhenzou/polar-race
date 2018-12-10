package com.alibabacloud.polar_race.engine.common;

import com.sun.jna.NativeLong;

/**
 * @since 0.1
 * create at 2018/11/22
 */
public interface Constants {

    /**
     * key 大小
     * 固定 8 byte
     */
    int KEY_SIZE = 8;

    /**
     * value 大小
     * 固定4K
     */
    int VALUE_SIZE = 4096;

    /**
     * JNA
     */
    NativeLong NATIVE_VALUE_SIZE = new NativeLong(VALUE_SIZE);

    /**
     * wal的value log 大小
     */
    int VALUE_LOG_ENTRY_SIZE = VALUE_SIZE + KEY_SIZE;

    /**
     * 保存value的文件名
     */
    String VALUE_FILE_NAME = "value.data";

    /**
     * 保存key的文件名
     */
    String KEY_FILE_NAME = "key.data";

    /**
     * 保存索引的文件名
     */
    String INDEX_FILE_NAME = "index.data";

    /**
     * 排序后的value文件名
     */
    String SORTED_VALUE_FILE_NAME = "sorted_value.data";

    /**
     * 排序后的key的文件名
     */
    String SORTED_KEY_FILE_NAME = "sorted_key.data";

    /**
     * 每个block的index数量
     */
    int BLOCK_INDEX_COUNT = 256 * 1024;

    /**
     * long -> long
     */
    int INDEX_ENTRY_SIZE = 16;

    /**
     * 每个block占用磁盘大小
     */
    int BLOCK_SIZE = BLOCK_INDEX_COUNT * INDEX_ENTRY_SIZE;

    /**
     * 写key的时候，每个mmap块的大小
     */
    long INDEX_MMAP_BLOCK_SIZE = 24 * 1024 * 1024;

    /**
     * index flush 或者 读取的时候每次读取的缓冲大小
     */
    int INDEX_FLUSH_BATCH_SIZE = 4 * 1024 * 1024;

    /**
     * 写的时候异步queue的大小
     */
    int QUEUE_SIZE = 256;

    /**
     * 4k对齐的key个数
     */
    int ALIGN_4k_KEY_COUNT = 4096 / KEY_SIZE;

    /**
     * 4k
     */
    int ALIGN_4k_KEY_BATCH = 4096;

    /**
     * key 4k对齐时value 的batch大小
     */
    int ALIGN_4k_VALUE_BATCH = ALIGN_4k_KEY_COUNT * VALUE_SIZE;
}
