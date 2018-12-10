package com.alibabacloud.polar_race.engine.common;

import java.util.ArrayList;

/**
 * @since 0.1
 * create at 2018/10/1
 */
public class EntryList extends ArrayList<Entry> {

    /**
     * 数据大小
     */
    public int dataSize;

    public EntryList(int cap) {
        super(cap);
    }

    @Override
    public boolean add(Entry entry) {
//        dataSize += entry.key.length + entry.value.length;
        return super.add(entry);
    }
}
