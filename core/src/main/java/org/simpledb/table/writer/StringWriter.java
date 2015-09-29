package org.simpledb.table.writer;

import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class StringWriter implements Writer<String> {
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

    @Override
    public int getSize(Comparable value) {
        return ((String) value).length() * 2 + 4;
    }
}
