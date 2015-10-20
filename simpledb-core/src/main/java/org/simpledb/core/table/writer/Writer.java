package org.simpledb.core.table.writer;

/**
 * Created by yvladimirov on 9/25/15.
 */
public interface Writer<T> {
    long write(long ptr, T value);

    int getSize(Comparable value);

}
