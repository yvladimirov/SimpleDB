package org.simpledb.core.table.reader;

/**
 * Created by yvladimirov on 9/25/15.
 */
public interface Reader<T extends Comparable> {

    public T read(long ptr);

    int getSize(long ptr);

}
