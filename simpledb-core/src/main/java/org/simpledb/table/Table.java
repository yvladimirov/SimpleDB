package org.simpledb.table;


import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;

import static org.simpledb.utils.UnsafeUtils.unsafe;


/**
 * Created by yvladimirov on 9/25/15.
 */
public class Table {
    private final String name;
    private final Fields fields;
    private TLongArrayList addresses = new TLongArrayList();
    private TIntObjectHashMap<Index> indexes = new TIntObjectHashMap<>();

    public Table(String name, Field[] fields) {
        this.name = name;
        this.fields = new Fields(fields);
        for (int i = 0; i < fields.length; i++) {
            Field f = fields[i];
            if (f.isIndexing()) {
                indexes.put(i, new Index(1000, this.fields, i));
            }
        }
    }


    public void add(Comparable... values) {
        if (values.length != fields.size())
            throw new RuntimeException("Values != Fields");

        long address = fields.write(values);
        for (Index index : indexes.valueCollection()) {
            index.put(values[index.getFieldNumber()], address);
        }
        addresses.add(address);
    }

    public Object[] findOne(int fieldNumber, Comparable value) {
        Index index = indexes.get(fieldNumber);
        if (index != null) {
            Index.Segment segment = index.get(value);
            long[] addresses = segment.getAddresses();
            for (int i = 0; i < segment.size(); i++) {
                Object[] result = fields.getValues(addresses[i]);
                if (result[fieldNumber].equals(value))
                    return result;
            }
        } else {
            return fullScan(fieldNumber, value);
        }
        return null;
    }

    public Comparable[] remove(int fieldNumber, Comparable value) {
        Index index = indexes.get(fieldNumber);
        long address = 0;
        if (index != null) {
            Index.Segment segment = index.get(value);
            long[] addresses = segment.getAddresses();
            for (int i = 0; i < segment.size(); i++) {
                Object[] result = fields.getValues(addresses[i]);
                if (result[fieldNumber].equals(value)) {
                    address = addresses[i];
                    break;
                }
            }
        } else {
            for (int i = 0; i < addresses.size(); i++) {
                Object[] result = fields.getValues(addresses.get(i));
                if (result[fieldNumber].equals(value)) {
                    address = addresses.get(i);
                    break;
                }
            }
        }
        addresses.remove(address);
        Comparable[] result = fields.getValues(address);
        for (Index i : indexes.valueCollection()) {
            i.remove(address, result[i.getFieldNumber()]);
        }
        unsafe.freeMemory(address);
        return result;
    }


    public void update(int fieldNumber,Comparable value ,Comparable... values){
        remove(fieldNumber,value);
        add(values);
    }

    private Object[] fullScan(int fieldNumber, Comparable value) {
        for (int i = 0; i < addresses.size(); i++) {
            Object[] result = fields.getValues(addresses.get(i));
            if (result[fieldNumber].equals(value))
                return result;
        }
        return null;
    }

    public void drop() {
        for (int i = 0; i < addresses.size(); i++) {
            unsafe.freeMemory(addresses.get(i));
        }
    }

}
