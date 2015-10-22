package com.simpledb.server.table;

import org.simpledb.core.builder.TableBuilder;
import org.simpledb.core.table.Table;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by yvladimirov on 10/21/15.
 */
public class TableRW implements Table {
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    private final Table table;


    public TableRW(TableBuilder tableBuilder) {
        this.table = tableBuilder.build();
    }

    @Override
    public String getName() {
        return table.getName();
    }

    @Override
    public void insert(Comparable... values) {
        try {
            writeLock.lock();
            table.insert(values);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Comparable[] findOne(int fieldNumber, Comparable value) {
        try {
            readLock.lock();
            return table.findOne(fieldNumber, value);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void update(int fieldNumber, Comparable value, Comparable... values) {
        try {
            writeLock.lock();
            table.update(fieldNumber, value, values);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Comparable[] delete(int fieldNumber, Comparable value) {
        try {
            writeLock.lock();
            return table.delete(fieldNumber, value);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void drop() {
        try {
            writeLock.lock();
            table.drop();
        } finally {
            writeLock.unlock();
        }
    }
}
