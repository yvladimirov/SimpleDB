package org.simpledb.table.reader;

import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class LongReader implements Reader<Long> {

    @Override
    public Long read(long ptr) {
        return unsafe.getLong(ptr);
    }

    @Override
    public int getSize(long ptr) {
        return 8;
    }
}