package org.simpledb.core.table.writer;

import org.simpledb.core.utils.UnsafeUtils;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class LongWriter implements Writer<Long> {
    @Override
    public long write(long ptr, Long value) {
        UnsafeUtils.unsafe.putLong(ptr, value);
        return ptr + 8;
    }

    @Override
    public int getSize(Comparable value) {
        return 8;
    }
}
