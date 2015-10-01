package org.simpledb.table.writer;

import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class LongWriter implements Writer<Long> {
    @Override
    public long write(long ptr, Long value) {
        unsafe.putLong(ptr, value);
        return ptr + 8;
    }

    @Override
    public int getSize(Comparable value) {
        return 8;
    }
}
