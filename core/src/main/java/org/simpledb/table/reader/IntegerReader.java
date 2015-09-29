package org.simpledb.table.reader;

import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class IntegerReader implements Reader {
    @Override
    public Comparable read(long address) {
        return unsafe.getInt(address);
    }

    @Override
    public long getSize(long ptr) {
        return 4;
    }
}