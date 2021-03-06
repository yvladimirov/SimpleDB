package org.simpledb.core.table.reader;


import org.simpledb.core.utils.UnsafeUtils;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class BooleanReader implements Reader {
    @Override
    public Comparable read(long ptr) {
        return UnsafeUtils.unsafe.getBoolean(null, ptr);
    }

    @Override
    public int getSize(long ptr) {
        return 1;
    }
}
