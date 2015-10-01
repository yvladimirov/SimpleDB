package org.simpledb.table.writer;

import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class IntegerWriter implements Writer {
    @Override
    public long write(long ptr, Object value) {
        unsafe.putInt(ptr, (Integer) value);
        return ptr + 4;
    }

    @Override
    public int getSize(Comparable value) {
        return 4;
    }
}
