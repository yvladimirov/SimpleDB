package org.simpledb.table;

import org.junit.Test;
import org.simpledb.table.reader.*;
import org.simpledb.table.writer.*;

import static org.junit.Assert.assertEquals;
import static org.simpledb.utils.UnsafeUtils.unsafe;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class ReadWriteTest {

    @Test
    public void booleanWriteReadTest() {
        long address = unsafe.allocateMemory(1);
        try {
            Writer writer = new BooleanWriter();
            Reader reader = new BooleanReader();

            assertEquals(1, writer.getSize(true));
            assertEquals(address + 1, writer.write(address, true));


            assertEquals(1, reader.getSize(address));
            assertEquals(true, reader.read(address));
        } finally {
            unsafe.freeMemory(address);
        }
    }

    @Test
    public void integerWriteReadTest() {
        long address = unsafe.allocateMemory(4);
        try {
            Writer writer = new IntegerWriter();
            Reader reader = new IntegerReader();

            assertEquals(4, writer.getSize(true));
            assertEquals(address + 4, writer.write(address, 6));


            assertEquals(4, reader.getSize(address));
            assertEquals(6, reader.read(address));
        } finally {
            unsafe.freeMemory(address);
        }
    }

    @Test
    public void longWriteReadTest() {
        long address = unsafe.allocateMemory(8);
        try {
            Writer writer = new LongWriter();
            Reader reader = new LongReader();

            assertEquals(8, writer.getSize(true));
            assertEquals(address + 8, writer.write(address, 10l));


            assertEquals(8, reader.getSize(address));
            assertEquals(10l, reader.read(address));
        } finally {
            unsafe.freeMemory(address);
        }
    }


    @Test
    public void stringWriteReadTest() {
        String test = "Hello world!";

        long address = unsafe.allocateMemory(test.length() * 2 + 4);
        try {
            Writer writer = new StringWriter();
            Reader reader = new StringReader();


            assertEquals(test.length() * 2 + 4, writer.getSize(test));
            assertEquals(address + test.length() * 2 + 4, writer.write(address, test));


            assertEquals(test.length() * 2 + 4, reader.getSize(address));
            assertEquals(test, reader.read(address));
        } finally {
            unsafe.freeMemory(address);
        }
    }
}
