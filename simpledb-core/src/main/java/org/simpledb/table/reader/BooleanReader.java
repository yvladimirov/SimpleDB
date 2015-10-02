package org.simpledb.table.reader;


import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class BooleanReader implements Reader {
    @Override
    public Comparable read(long ptr) {
        return unsafe.getBoolean(null, ptr);
    }

    @Override
    public int getSize(long ptr) {
        return 1;
    }
}
