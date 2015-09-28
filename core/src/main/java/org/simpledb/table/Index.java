package org.simpledb.table;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import javafx.util.Pair;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

/**
 * Created by yvladimirov on 9/26/15.
 */
public class Index {
    private final static Comparator COMPARATOR = new Comparator<Pair<Long, Comparable>>() {
        @Override
        public int compare(Pair<Long, Comparable> o1, Pair<Long, Comparable> o2) {
            return o1.getValue().compareTo(o2.getValue());
        }
    };
    private final RangeMap<Comparable, Entry> map = TreeRangeMap.create();
    private final int MAX_ELEMENT;
    private final Reader reader;
    private final long offset;
    private final int fieldNumber;

    public Index(int capacity, Reader reader, long offset, int fieldNumber) {
        this.MAX_ELEMENT = capacity;
        this.reader = reader;
        this.offset = offset;
        this.fieldNumber = fieldNumber;
        map.put(Range.all(), new Entry());
    }

    public void put(Comparable key, long value) {
        Map.Entry<Range<Comparable>, Entry> mapEntry = map.getEntry(key);
        if (mapEntry.getValue().isFull()) {
            split(mapEntry);
            map.getEntry(key).getValue().add(value);
        } else {
            mapEntry.getValue().add(value);
        }
    }

    private void split(Map.Entry<Range<Comparable>, Entry> mapEntry) {
        map.remove(mapEntry.getKey());

        Pair<Long, Comparable>[] pairs = new Pair[MAX_ELEMENT];
        for (int i = 0; i < MAX_ELEMENT; i++) {
            long address = mapEntry.getValue().address[i];
            pairs[i] = new Pair(address, reader.read(address + offset));
        }

        Arrays.sort(pairs, COMPARATOR);

        int center = MAX_ELEMENT / 2-1;

        Entry entry1 = new Entry();
        Entry entry2 = new Entry();
        for (int i = 0; i <= center; i++) {
            entry1.add(pairs[i].getKey());
            entry2.add(pairs[i + center + 1].getKey());
        }

        Range<Comparable> r1 = Range.atMost(pairs[center].getValue()).intersection(mapEntry.getKey());
        Range<Comparable> r2 = Range.greaterThan(pairs[center].getValue()).intersection(mapEntry.getKey());
        map.put(r1, entry1);
        map.put(r2, entry2);
    }

    public Entry get(Comparable key) {
        Map.Entry<Range<Comparable>, Entry> mapEntry = map.getEntry(key);
        return mapEntry.getValue();
    }

    public int getFieldNumber() {
        return fieldNumber;
    }

    public class Entry {
        private long[] address = new long[MAX_ELEMENT];
        private int count;

        public boolean isFull() {
            return count >= MAX_ELEMENT;
        }

        public void add(long value) {
            address[count] = value;
            count++;
        }

        public int size() {
            return count;
        }

        public long[] getAddresses() {
            return address;
        }
    }
}
