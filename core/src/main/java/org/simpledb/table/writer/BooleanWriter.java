package org.simpledb.table.writer;

import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class BooleanWriter implements Writer {
    @Override
    public long write(long ptr, Object value) {
        unsafe.putBoolean(null, ptr, (Boolean) value);
        return ptr + 1;
    }

    @Override
    public int getSize(Comparable value) {
        return 1;
    }
}
