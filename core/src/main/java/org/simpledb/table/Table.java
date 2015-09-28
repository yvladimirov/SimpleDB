package org.simpledb.table;


import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;

import static org.simpledb.utils.UnsafeUtils.unsafe;


/**
 * Created by yvladimirov on 9/25/15.
 */
public class Table {
    private final Field[] fields;
    private final long allocated;
    private TLongArrayList addresses = new TLongArrayList();
    private TIntObjectHashMap<Index> indexes = new TIntObjectHashMap<>();

    public Table(Field[] fields) {
        this.fields = fields;
        long allocated = 0;
        for (int i = 0; i < fields.length; i++) {
            Field f = fields[i];
            if (f.isIndexing()) {
                indexes.put(i, new Index(1000, f.getReader(), allocated, i));
            }
            allocated += f.getSize();
        }
        this.allocated = allocated;
    }

    public void add(Comparable... values) {
        if (values.length != fields.length)
            throw new RuntimeException("Values != Fields");

        long address = unsafe.allocateMemory(allocated);
        long offset = address;
        for (int i = 0; i < fields.length; i++) {
            fields[i].write(offset, values[i]);
            offset += fields[i].getSize();
        }
        for (Index index : indexes.valueCollection()) {
            index.put(values[index.getFieldNumber()], address);
        }
        addresses.add(address);
    }


    public Object[] findOne() {
        Object[] result = new Object[fields.length];
        for (int i = 0; i < addresses.size(); i++) {
            long address = addresses.get(i);

            for (int y = 0; y < fields.length; y++) {
                result[y] = fields[y].read(address);
                address += fields[y].getSize();
            }
            break;
        }
        return result;
    }

    public Object[] findOne(int fieldNumber, Comparable value) {
        Index index = indexes.get(fieldNumber);
        if (index != null) {
            Index.Entry entry = index.get(value);
            long[] addresses = entry.getAddresses();
            for (int i = 0; i < entry.size(); i++) {
                Object[] result = read(addresses[i]);
                if (result[fieldNumber].equals(value))
                    return result;
            }
        } else {
            for (int i = 0; i < addresses.size(); i++) {
                Object[] result = read(addresses.get(i));
                if (result[fieldNumber].equals(value))
                    return result;
            }
        }
        return null;
    }

    private Object[] read(long address) {
        Object[] result = new Object[fields.length];
        for (int y = 0; y < fields.length; y++) {
            result[y] = fields[y].read(address);
            address += fields[y].getSize();
        }
        return result;
    }

    public void drop() {
        for (int i = 0; i < addresses.size(); i++) {
            unsafe.freeMemory(addresses.get(i));
        }
    }

    public TIntObjectHashMap<Index> getIndexes() {
        return indexes;
    }
}
