package org.simpledb.table.writer;

import java.nio.ByteBuffer;

import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class BytesWriter implements Writer {
    private static final int BYTES_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);

    @Override
    public long write(long address, Object value) {
        byte[] buffer = ((ByteBuffer) value).array();
        unsafe.putInt(address, buffer.length);
        address += 4;
        unsafe.copyMemory(buffer, BYTES_ARRAY_OFFSET, null, address, buffer.length);
        return address + buffer.length;
    }

    @Override
    public int getSize(Comparable value) {
        return ((ByteBuffer) value).array().length + 4;
    }
}
