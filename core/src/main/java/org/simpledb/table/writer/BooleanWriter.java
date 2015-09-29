package org.simpledb.table.writer;

import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class BooleanWriter implements Writer {
    @Override
    public long write(long address, Object value) {
        unsafe.putBoolean(null, address, (Boolean) value);
        return address + 1;
    }

    @Override
    public int getSize(Comparable value) {
        return 1;
    }
}
