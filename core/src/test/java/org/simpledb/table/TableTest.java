package org.simpledb.table;

import org.junit.Test;
import org.simpledb.builder.FieldBuilder;
import org.simpledb.builder.TableBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by yvladimirov on 9/27/15.
 */
public class TableTest {

    @Test
    public void testLongTablePutAndGetNotIndex() {
        Table table = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("long").type(FieldType.LONG).build())
                .build();
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
        Table table = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("long").type(FieldType.LONG).build())
                .addField(FieldBuilder.builder().name("long2").type(FieldType.LONG).build())
                .build();
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
        Table table = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("long").type(FieldType.LONG).indexing(true).build())
                .build();
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
        Table table = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("long").type(FieldType.LONG).indexing(true).build())
                .addField(FieldBuilder.builder().name("long2").type(FieldType.LONG).indexing(true).build())
                .build();
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
        Table table = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("string").type(FieldType.STRING).build())
                .build();
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
        Table table = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("string").type(FieldType.STRING).build())
                .addField(FieldBuilder.builder().name("string2").type(FieldType.STRING).build())
                .build();

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
        Table table = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("string").type(FieldType.STRING).indexing(true).build())
                .build();

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
        Table table = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("string").type(FieldType.STRING).indexing(true).build())
                .addField(FieldBuilder.builder().name("string2").type(FieldType.STRING).indexing(true).build())
                .build();

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
        Table table = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("long").type(FieldType.LONG).indexing(true).build())
                .build();

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
        Table table = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("long").type(FieldType.LONG).indexing(true).build())
                .build();

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
        Table table = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("string").type(FieldType.STRING).indexing(true).build())
                .build();

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
