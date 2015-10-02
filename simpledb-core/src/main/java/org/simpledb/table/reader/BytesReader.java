package org.simpledb.table.reader;

import java.nio.ByteBuffer;

import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class BytesReader implements Reader {
    private static final int BYTES_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);

    @Override
    public Comparable read(long ptr) {
        byte[] buffer = new byte[unsafe.getInt(ptr)];
        ptr += 4;
        unsafe.copyMemory(null, ptr, buffer, BYTES_ARRAY_OFFSET, buffer.length);
        return ByteBuffer.wrap(buffer);
    }

    @Override
    public int getSize(long ptr) {
        return unsafe.getInt(ptr) + 4;
    }
}
