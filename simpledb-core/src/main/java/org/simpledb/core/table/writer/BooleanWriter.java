package org.simpledb.core.table.writer;

import org.simpledb.core.utils.UnsafeUtils;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class BooleanWriter implements Writer {
    @Override
    public long write(long ptr, Object value) {
        UnsafeUtils.unsafe.putBoolean(null, ptr, (Boolean) value);
        return ptr + 1;
    }

    @Override
    public int getSize(Comparable value) {
        return 1;
    }
}
