package org.simpledb.table.reader;

import static org.simpledb.utils.UnsafeUtils.unsafe;


/**
 * Created by yvladimirov on 9/25/15.
 */
public interface Reader<T extends Comparable> {

    public T read(long address);

    long getSize(long ptr);

}
