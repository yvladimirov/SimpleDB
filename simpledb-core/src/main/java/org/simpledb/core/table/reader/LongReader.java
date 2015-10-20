package org.simpledb.core.table.reader;

import org.simpledb.core.utils.UnsafeUtils;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class LongReader implements Reader<Long> {

    @Override
    public Long read(long ptr) {
        return UnsafeUtils.unsafe.getLong(ptr);
    }

    @Override
    public int getSize(long ptr) {
        return 8;
    }
}