package org.simpledb.table.writer;

import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/25/15.
 */
public interface Writer<T> {
    long write(long address, T value);

    int getSize(Comparable value);

}
