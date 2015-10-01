package org.simpledb.table.reader;

import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class StringReader implements Reader<String> {

    @Override
    public String read(long ptr) {
        int length = unsafe.getInt(ptr);
        ptr += 4;
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = unsafe.getChar(ptr);
            ptr += 2;
        }
        return new String(chars);
    }

    @Override
    public int getSize(long ptr) {
        int length = unsafe.getInt(ptr);
        return length * 2 + 4;
    }
}
