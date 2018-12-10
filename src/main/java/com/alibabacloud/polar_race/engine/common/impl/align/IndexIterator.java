package com.alibabacloud.polar_race.engine.common.impl.align;

import com.alibabacloud.polar_race.engine.common.Constants;
import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.index.Index;
import com.alibabacloud.polar_race.engine.common.util.Convert;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibabacloud.polar_race.engine.common.util.Utils.log;

/**
 * not thread safe
 *
 * @since 0.1
 * create at 2018/11/22
 */
public class IndexIterator implements Iterator<Index> {

    private static AtomicInteger COUNTER = new AtomicInteger();

    private byte[] lower;
    private byte[] upper;

    private byte[] indexes;

    /**
     * current offset of iterator
     * start with 0
     */
    private int current;

    /**
     * max
     */
    private int max;

    private int id;

    /**
     * index holder
     * 总是会复用
     */
    private Index index = new Index();

    /**
     * zero element
     */
    public IndexIterator() {
    }

    public IndexIterator(String path, byte[] indexes, byte[] lower, byte[] upper) {
        id = COUNTER.getAndIncrement();
        this.indexes = indexes;
        this.lower = lower;
        this.upper = upper;
        init();
        log("iterator %s %d current %d  max %d", path, id, current, max);
    }

    private void init() {
        // TODO 特殊优化
        int count = indexes.length / Constants.INDEX_ENTRY_SIZE;
        current = 0;
        if (lower != null) {
            current = search(lower, count, EngineRace.KEY_COMPARATOR);
        }
        max = count;
        if (upper != null) {
            max = search(upper, count, EngineRace.KEY_COMPARATOR);
        }
    }

    /**
     * search the index of target or smallest value which larger than target in values
     *
     * @param target     target
     * @param comparator comparator
     * @return index
     */
    private int search(byte[] target, int count, Comparator<byte[]> comparator) {
        int i = 0;
        int j = count;
        byte[] midValue = new byte[Constants.KEY_SIZE];
        byte[] block = this.indexes;
        while (i < j) {
            int mid = (i + j) >> 1;
            System.arraycopy(block, mid * Constants.INDEX_ENTRY_SIZE, midValue, 0, Constants.KEY_SIZE);
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

    /**
     * 暂时就假设用户一定循环到hasNext返回false为止
     * <p>
     *
     * @return hasNext
     */
    @Override
    public boolean hasNext() {
        return current < max;
    }

    /**
     * 如果没有调用hasNext判断，那么返回的结果是未定义的
     *
     * @return index
     */
    @Override
    public Index next() {
        int offset = current * Constants.INDEX_ENTRY_SIZE;
        System.arraycopy(indexes, offset, Convert.long2bytes(index.key), 0, Constants.KEY_SIZE);
        index.address = Convert.bytes2long(indexes, offset + Constants.KEY_SIZE);
        current++;
        return index;
    }
}
