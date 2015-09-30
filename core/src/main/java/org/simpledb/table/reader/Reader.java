package org.simpledb.table.reader;

/**
 * Created by yvladimirov on 9/25/15.
 */
public interface Reader<T extends Comparable> {

    public T read(long address);

    int getSize(long ptr);

}
