package org.simpledb.table;

import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/25/15.
 */
public interface Writer<T> {
    long write(long address, T value);


    public static class LongWriter implements Writer<Long> {
        @Override
        public long write(long address, Long value) {
            unsafe.putLong(address, value);
            return address + 8;
        }
    }

    public static class StringWriter implements Writer<String> {
        @Override
        public long write(long address, String value) {

            unsafe.putInt(address, value.length());
            address += 4;
            for (int i = 0; i < value.length(); i++) {
                unsafe.putChar(address, value.charAt(i));
                address += 2;
            }
            return address;
        }
    }
}
