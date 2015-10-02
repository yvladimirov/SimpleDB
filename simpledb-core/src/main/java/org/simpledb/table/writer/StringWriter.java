package org.simpledb.table.writer;

import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class StringWriter implements Writer<String> {
    @Override
    public long write(long ptr, String value) {

        unsafe.putInt(ptr, value.length());
        ptr += 4;
        for (int i = 0; i < value.length(); i++) {
            unsafe.putChar(ptr, value.charAt(i));
            ptr += 2;
        }
        return ptr;
    }

    @Override
    public int getSize(Comparable value) {
        return ((String) value).length() * 2 + 4;
    }
}
