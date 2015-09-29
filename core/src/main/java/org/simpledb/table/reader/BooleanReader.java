package org.simpledb.table.reader;


import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class BooleanReader implements Reader {
    @Override
    public Comparable read(long address) {
        return unsafe.getBoolean(null,address);
    }

    @Override
    public long getSize(long ptr) {
        return 1;
    }
}
