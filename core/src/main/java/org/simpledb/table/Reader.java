package org.simpledb.table;

import static org.simpledb.utils.UnsafeUtils.unsafe;


/**
 * Created by yvladimirov on 9/25/15.
 */
public interface Reader<T extends Comparable> {

    public T read(long address);


    public static class LongReader implements Reader<Long> {

        @Override
        public Long read(long address) {
            return unsafe.getLong(address);
        }
    }

    public static class StringReader implements Reader<String> {

        @Override
        public String read(long address) {
            int length = unsafe.getInt(address);
            address += 4;
            char[] chars = new char[length];
            for (int i = 0; i < length; i++) {
                chars[i] = unsafe.getChar(address);
                address += 2;
            }
            return new String(chars);
        }
    }
}
