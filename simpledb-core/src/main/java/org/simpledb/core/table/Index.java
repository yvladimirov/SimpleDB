package org.simpledb.core.table;

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
    private final RangeMap<Comparable, Segment> map = TreeRangeMap.create();
    private final int MAX_ELEMENT;
    private final Fields fields;
    private final int fieldNumber;

    public Index(int capacity, Fields fields, int fieldNumber) {
        this.MAX_ELEMENT = capacity;
        this.fields = fields;
        this.fieldNumber = fieldNumber;
        map.put(Range.all(), new Segment());
    }

    public void put(Comparable key, long value) {
        Map.Entry<Range<Comparable>, Segment> mapEntry = map.getEntry(key);
        if (mapEntry.getValue().isFull()) {
            split(mapEntry);
            map.getEntry(key).getValue().add(value);
        } else {
            mapEntry.getValue().add(value);
        }
    }

    private void split(Map.Entry<Range<Comparable>, Segment> mapEntry) {
        map.remove(mapEntry.getKey());

        Pair<Long, Comparable>[] pairs = new Pair[MAX_ELEMENT];
        for (int i = 0; i < MAX_ELEMENT; i++) {
            long address = mapEntry.getValue().addresses[i];
            pairs[i] = new Pair(address, fields.getValue(address, fieldNumber));
        }

        Arrays.sort(pairs, COMPARATOR);

        int center = MAX_ELEMENT / 2 - 1;

        Segment segment1 = new Segment();
        Segment segment2 = new Segment();
        for (int i = 0; i <= center; i++) {
            segment1.add(pairs[i].getKey());
            segment2.add(pairs[i + center + 1].getKey());
        }

        Range<Comparable> r1 = Range.atMost(pairs[center].getValue()).intersection(mapEntry.getKey());
        Range<Comparable> r2 = Range.greaterThan(pairs[center].getValue()).intersection(mapEntry.getKey());
        map.put(r1, segment1);
        map.put(r2, segment2);
    }

    public Segment get(Comparable key) {
        Map.Entry<Range<Comparable>, Segment> mapEntry = map.getEntry(key);
        return mapEntry.getValue();
    }

    public int getFieldNumber() {
        return fieldNumber;
    }

    public void remove(long address, Comparable comparable) {
        Map.Entry<Range<Comparable>, Segment> entry = map.getEntry(comparable);
        entry.getValue().remove(address);
        if (entry.getValue().isEmpty()) {

            for (Map.Entry<Range<Comparable>, Segment> rs : map.asMapOfRanges().entrySet()) {
                if (!rs.getKey().equals(entry.getKey()) && rs.getKey().isConnected(entry.getKey())) {
                    map.put(rs.getKey().span(entry.getKey()), rs.getValue());
                    break;
                }
            }
        }
    }

    public class Segment {
        private long[] addresses = new long[MAX_ELEMENT];
        private int count;

        public boolean isFull() {
            return count >= MAX_ELEMENT;
        }

        public boolean isEmpty() {
            return count == 0;
        }

        public void add(long value) {
            addresses[count] = value;
            count++;
        }

        public int size() {
            return count;
        }

        public long[] getAddresses() {
            return addresses;
        }

        public void remove(long address) {
            for (int i = 0; i < count; i++) {
                if (address == addresses[i]) {
                    System.arraycopy(addresses, i + 1, addresses, i, count - i - 1);
                    count--;
                    addresses[count] = 0;
                    break;
                }
            }
        }
    }
}
