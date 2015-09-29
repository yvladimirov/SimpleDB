package org.simpledb.table;

import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/29/15.
 */
public class Fields {
    private final Field[] fields;


    public Fields(Field[] fields) {
        this.fields = fields;
    }


    public Comparable[] getValues(long ptr) {
        Comparable[] result = new Comparable[fields.length];
        for (int i = 0; i < fields.length; i++) {
            result[i] = fields[i].read(ptr);
            ptr += fields[i].getSize(ptr);
        }
        return result;
    }


    public Comparable getValue(long ptr, int fieldNumber) {
        for (int i = 0; i < fieldNumber; i++) {
            ptr += fields[i].getSize(ptr);
        }
        return fields[fieldNumber].read(ptr);
    }

    public int size() {
        return fields.length;
    }

    public long write(Comparable[] values) {
        long size = 0;
        for (int i = 0; i < fields.length; i++) {
            size += fields[i].getSize(values[i]);
        }
        long address = unsafe.allocateMemory(size);
        long offset = address;

        for (int i = 0; i < fields.length; i++) {
            fields[i].write(offset, values[i]);
            offset += fields[i].getSize(values[i]);
        }
        return address;
    }
}
