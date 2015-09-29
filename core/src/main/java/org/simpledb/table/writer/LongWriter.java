package org.simpledb.table.writer;

import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class LongWriter implements Writer<Long> {
    @Override
    public long write(long address, Long value) {
        unsafe.putLong(address, value);
        return address + 8;
    }

    @Override
    public int getSize(Comparable value) {
        return 8;
    }
}
