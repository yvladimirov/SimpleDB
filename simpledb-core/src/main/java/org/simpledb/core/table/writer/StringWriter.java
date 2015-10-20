package org.simpledb.core.table.writer;

import org.simpledb.core.utils.UnsafeUtils;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class StringWriter implements Writer<String> {
    @Override
    public long write(long ptr, String value) {

        UnsafeUtils.unsafe.putInt(ptr, value.length());
        ptr += 4;
        for (int i = 0; i < value.length(); i++) {
            UnsafeUtils.unsafe.putChar(ptr, value.charAt(i));
            ptr += 2;
        }
        return ptr;
    }

    @Override
    public int getSize(Comparable value) {
        return ((String) value).length() * 2 + 4;
    }
}
