package org.simpledb.table.writer;

/**
 * Created by yvladimirov on 9/25/15.
 */
public interface Writer<T> {
    long write(long address, T value);

    int getSize(Comparable value);

}
