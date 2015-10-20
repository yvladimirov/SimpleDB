package org.simpledb.core.table.reader;

import org.simpledb.core.utils.UnsafeUtils;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class IntegerReader implements Reader {
    @Override
    public Comparable read(long ptr) {
        return UnsafeUtils.unsafe.getInt(ptr);
    }

    @Override
    public int getSize(long ptr) {
        return 4;
    }
}
