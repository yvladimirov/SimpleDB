package org.simpledb.core.table.reader;

import org.simpledb.core.utils.UnsafeUtils;

import java.nio.ByteBuffer;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class BytesReader implements Reader {
    private static final int BYTES_ARRAY_OFFSET = UnsafeUtils.unsafe.arrayBaseOffset(byte[].class);

    @Override
    public Comparable read(long ptr) {
        byte[] buffer = new byte[UnsafeUtils.unsafe.getInt(ptr)];
        ptr += 4;
        UnsafeUtils.unsafe.copyMemory(null, ptr, buffer, BYTES_ARRAY_OFFSET, buffer.length);
        return ByteBuffer.wrap(buffer);
    }

    @Override
    public int getSize(long ptr) {
        return UnsafeUtils.unsafe.getInt(ptr) + 4;
    }
}
