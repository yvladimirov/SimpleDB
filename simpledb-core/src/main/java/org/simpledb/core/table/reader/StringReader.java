package org.simpledb.core.table.reader;

import org.simpledb.core.utils.UnsafeUtils;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class StringReader implements Reader<String> {

    @Override
    public String read(long ptr) {
        int length = UnsafeUtils.unsafe.getInt(ptr);
        ptr += 4;
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = UnsafeUtils.unsafe.getChar(ptr);
            ptr += 2;
        }
        return new String(chars);
    }

    @Override
    public int getSize(long ptr) {
        int length = UnsafeUtils.unsafe.getInt(ptr);
        return length * 2 + 4;
    }
}
