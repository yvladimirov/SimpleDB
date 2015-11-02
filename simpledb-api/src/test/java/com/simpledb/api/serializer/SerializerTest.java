package com.simpledb.api.serializer;

import com.simpledb.api.Request;
import com.simpledb.api.messages.CreateTableMessage;
import com.simpledb.api.messages.DeleteMessage;
import com.simpledb.api.messages.DropTableMessage;
import com.simpledb.api.messages.InsertMessage;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by yvladimirov on 10/25/15.
 */
public class SerializerTest {
    private Serializer serializer = new Serializer();


    @Test
    public void testRequestDropTableMessage() throws Exception {
        Request request = new Request(1, new DropTableMessage("testDrop"));

        byte[] bytes = serializer.serialize(request);

        request = serializer.deserialize(bytes, Request.class);
        assertEquals(1, request.getRequestId());
        assertTrue(request.getMessage() instanceof DropTableMessage);
        assertEquals("testDrop", ((DropTableMessage) request.getMessage()).getTableName());
    }

    @Test
    public void testCreateTableMessage() throws Exception {
        CreateTableMessage message = new CreateTableMessage("testTable");
//
        message.addField("field1", CreateTableMessage.FieldType.INTEGER, false);
        message.addField("field2", CreateTableMessage.FieldType.LONG, false);
        message.addField("field3", CreateTableMessage.FieldType.STRING, true);

        byte[] bytes = serializer.serialize(new Request(1, message));

        Request request = serializer.deserialize(bytes, Request.class);

        message = ((CreateTableMessage) request.getMessage());

        assertEquals(1, request.getRequestId());
        assertTrue(request.getMessage() instanceof CreateTableMessage);
        assertEquals("testTable", message.getTableName());

        assertEquals(3, message.getFields().size());
//
        assertEquals("field1", message.getFields().get(0).getFieldName());
        assertEquals(CreateTableMessage.FieldType.INTEGER, message.getFields().get(0).getType());
        assertFalse(message.getFields().get(0).isIndexing());

        assertEquals("field2", message.getFields().get(1).getFieldName());
        assertEquals(CreateTableMessage.FieldType.LONG, message.getFields().get(1).getType());
        assertFalse(message.getFields().get(1).isIndexing());

        assertEquals("field3", message.getFields().get(2).getFieldName());
        assertEquals(CreateTableMessage.FieldType.STRING, message.getFields().get(2).getType());
        assertTrue(message.getFields().get(2).isIndexing());
    }

    @Test
    public void testInsertMessage() throws Exception {
        InsertMessage message = new InsertMessage("testTable");
        message.addField("field1", 1).addField("field2", "1");

        byte[] bytes = serializer.serialize(new Request(5, message));

        Request request = serializer.deserialize(bytes, Request.class);

        assertEquals(5, request.getRequestId());
        assertTrue(request.getMessage() instanceof InsertMessage);
        assertEquals("testTable", message.getTableName());

        message = (InsertMessage) request.getMessage();

        assertEquals("testTable", message.getTableName());
        assertEquals(2, message.getFields().size());
        assertEquals(1, message.getFields().get("field1"));
        assertEquals("1", message.getFields().get("field2"));
    }

    @Test
    public void testDeleteMessage() throws Exception {
        DeleteMessage message = new DeleteMessage("testTable");
        message.addField("field1", 1).addField("field2", "1");

        byte[] bytes = serializer.serialize(new Request(5, message));

        Request request = serializer.deserialize(bytes, Request.class);

        assertEquals(5, request.getRequestId());
        assertTrue(request.getMessage() instanceof DeleteMessage);
        assertEquals("testTable", message.getTableName());

        message = (DeleteMessage) request.getMessage();

        assertEquals("testTable", message.getTableName());
        assertEquals(2, message.getFields().size());
        assertEquals(1, message.getFields().get("field1"));
        assertEquals("1", message.getFields().get("field2"));
    }
}
