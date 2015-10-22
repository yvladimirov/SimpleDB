package org.simpledb.core.table;

import org.junit.Test;
import org.simpledb.core.builder.FieldBuilder;
import org.simpledb.core.builder.TableBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by yvladimirov on 9/27/15.
 */
public class TableUnsafeTest {

    @Test
    public void testLongTablePutAndGetNotIndex() {
        Table table = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("long").type(FieldType.LONG).build())
                .build();
        table.insert(1l);

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
        table.insert(1l, 5l);

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
        table.insert(1l);

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
        table.insert(1l, 5l);

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
        table.insert("1l");

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

        table.insert("1l", "5l");

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

        table.insert("1l");

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

        table.insert("1l", "5l");

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
            table.insert(l);
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
            table.insert(l);
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
            table.insert("" + l);
        }

        for (long l = 0; l < 1000000; l++) {
            Object[] result = table.findOne(0, "" + l);
            assertEquals(1, result.length);
            assertEquals("" + l, result[0]);
        }

        table.drop();

    }


    @Test
    public void testDeleteString() {
        Table table = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("string").type(FieldType.STRING).indexing(true).build())
                .build();

        table.insert("test");

        Object[] result = table.findOne(0, "test");
        assertEquals(1, result.length);
        assertEquals("test", result[0]);

        result = table.delete(0, "test");
        assertEquals(1, result.length);
        assertEquals("test", result[0]);

        result = table.findOne(0, "test");
        assertNull(result);

        table.drop();

    }

    @Test
    public void testDeleteIntegerSplit() {
        Table table = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("int").type(FieldType.INTEGER).indexing(true).build())
                .build();

        for (int i = 0; i <= 1000; i++)
            table.insert(i);

        for (int i = 499; i <= 1000; i++) {
            Object[] result = table.delete(0, i);
            assertEquals(1, result.length);
            assertEquals(i, result[0]);
        }

        for (int i = 499; i <= 1000; i++)
            table.insert(i);

        for (int i = 0; i <= 499; i++) {
            Object[] result = table.delete(0, i);
            assertEquals(1, result.length);
            assertEquals(i, result[0]);
        }

        table.drop();
    }

    @Test
    public void testUpdate() {
        Table table = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("int").type(FieldType.INTEGER).indexing(true).build())
                .addField(FieldBuilder.builder().name("long").type(FieldType.LONG).indexing(true).build())
                .addField(FieldBuilder.builder().name("string").type(FieldType.STRING).indexing(true).build())
                .build();


        table.insert(1, 2l, "test");

        table.update(0, 1, 1, 5l, "Test2");

        Object[] result = table.findOne(0, 1);

        assertEquals(3, result.length);
        assertEquals(1, result[0]);
        assertEquals(5l, result[1]);
        assertEquals("Test2", result[2]);

        table.drop();
    }
}
