package org.simpledb.table;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by yvladimirov on 9/27/15.
 */
public class TableTest {

    @Test
    public void testLongTablePutAndGetNotIndex() {
        Table table = new Table(new Field[]{new Field(FieldType.LONG, "long", false)});
        table.add(1l);

        Object[] result = table.findOne(0, 1l);
        assertEquals(1, result.length);
        assertEquals(1l, result[0]);

        result = table.findOne(0, 2l);

        assertNull(result);

        table.drop();
    }

    @Test
    public void testLongTablePutAndGetNotIndexTwoField() {
        Table table = new Table(new Field[]{new Field(FieldType.LONG, "long", false), new Field(FieldType.LONG, "long2", false)});
        table.add(1l, 5l);

        Object[] result = table.findOne(0, 1l);
        assertEquals(2, result.length);
        assertEquals(1l, result[0]);
        assertEquals(5l, result[1]);

        result = table.findOne(0, 5l);
        assertNull(result);

        result = table.findOne(1, 5l);
        assertEquals(1l, result[0]);
        assertEquals(5l, result[1]);

        table.drop();
    }


    @Test
    public void testLongTablePutAndGet() {
        Table table = new Table(new Field[]{new Field(FieldType.LONG, "long", true)});
        table.add(1l);

        Object[] result = table.findOne(0, 1l);
        assertEquals(1, result.length);
        assertEquals(1l, result[0]);

        result = table.findOne(0, 2l);

        assertNull(result);

        table.drop();
    }

    @Test
    public void testLongTablePutAndGetTwoField() {
        Table table = new Table(new Field[]{new Field(FieldType.LONG, "long", true), new Field(FieldType.LONG, "long2", true)});
        table.add(1l, 5l);

        Object[] result = table.findOne(0, 1l);
        assertEquals(2, result.length);
        assertEquals(1l, result[0]);
        assertEquals(5l, result[1]);

        result = table.findOne(0, 5l);
        assertNull(result);

        result = table.findOne(1, 5l);
        assertEquals(1l, result[0]);
        assertEquals(5l, result[1]);

        table.drop();
    }


    @Test
    public void testStringTablePutAndGetNotIndex() {
        Table table = new Table(new Field[]{new Field(FieldType.STRING, "string", false)});
        table.add("1l");

        Object[] result = table.findOne(0, "1l");
        assertEquals(1, result.length);
        assertEquals("1l", result[0]);

        result = table.findOne(0, "2l");

        assertNull(result);

        table.drop();
    }

    @Test
    public void testStringTablePutAndGetNotIndexTwoField() {
        Table table = new Table(new Field[]{new Field(FieldType.STRING, "string", false), new Field(FieldType.STRING, "string2", false)});
        table.add("1l", "5l");

        Object[] result = table.findOne(0, "1l");
        assertEquals(2, result.length);
        assertEquals("1l", result[0]);
        assertEquals("5l", result[1]);

        result = table.findOne(0, "5l");
        assertNull(result);

        result = table.findOne(1, "5l");
        assertEquals("1l", result[0]);
        assertEquals("5l", result[1]);

        table.drop();
    }


    @Test
    public void testStringTablePutAndGet() {
        Table table = new Table(new Field[]{new Field(FieldType.STRING, "string", true)});
        table.add("1l");

        Object[] result = table.findOne(0, "1l");
        assertEquals(1, result.length);
        assertEquals("1l", result[0]);

        result = table.findOne(0, "2l");

        assertNull(result);

        table.drop();
    }

    @Test
    public void testStringTablePutAndGetTwoField() {
        Table table = new Table(new Field[]{new Field(FieldType.STRING, "string", true), new Field(FieldType.STRING, "string2", true)});
        table.add("1l", "5l");

        Object[] result = table.findOne(0, "1l");
        assertEquals(2, result.length);
        assertEquals("1l", result[0]);
        assertEquals("5l", result[1]);

        result = table.findOne(0, "5l");
        assertNull(result);

        result = table.findOne(1, "5l");
        assertEquals("1l", result[0]);
        assertEquals("5l", result[1]);

        table.drop();
    }

    @Test
    public void testLongKK() {
        Table table = new Table(new Field[]{new Field(FieldType.LONG, "long", true)});

        for (long l = 0; l < 1000000; l++) {
            table.add(l);
        }

        Object[] result = table.findOne(0, 100000l);
        assertEquals(1, result.length);
        assertEquals(100000l, result[0]);


        table.drop();

    }

    @Test
    public void testLongSplitingIndex() {
        Table table = new Table(new Field[]{new Field(FieldType.LONG, "long", true)});

        for (long l = 0; l < 1000000; l++) {
            table.add(l);
        }

        for (long l = 0; l < 1000000; l++) {
            Object[] result = table.findOne(0, l);
            assertEquals(1, result.length);
            assertEquals(l, result[0]);
        }

        table.drop();

    }

    @Test
    public void testStringSplitingIndex() {
        Table table = new Table(new Field[]{new Field(FieldType.STRING, "string", true)});

        for (long l = 0; l < 1000000; l++) {
            table.add("" + l);
        }

        for (long l = 0; l < 1000000; l++) {
            Object[] result = table.findOne(0, "" + l);
            assertEquals(1, result.length);
            assertEquals("" + l, result[0]);
        }

        table.drop();

    }
}
