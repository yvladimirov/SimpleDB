package com.simpledb.api.serializer;

import com.simpledb.api.Request;
import com.simpledb.api.Response;
import com.simpledb.api.messages.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by yvladimirov on 10/25/15.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SerializerTest {
    private Serializer serializer = new Serializer();


    @Test
    public void testRequestDropTableMessage() throws Exception {
        Request request = new Request(new DropTableMessage("testDrop"));

        byte[] bytes = serializer.serialize(request);

        request = serializer.deserialize(bytes, Request.class);
        assertEquals(4, request.getRequestId());
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

        byte[] bytes = serializer.serialize(new Request(message));

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
        message.addField(1, 1).addField(2, "1");

        byte[] bytes = serializer.serialize(new Request(message));

        Request request = serializer.deserialize(bytes, Request.class);

        assertEquals(3, request.getRequestId());
        assertTrue(request.getMessage() instanceof InsertMessage);
        assertEquals("testTable", message.getTableName());

        message = (InsertMessage) request.getMessage();

        assertEquals("testTable", message.getTableName());
        assertEquals(2, message.getFields().size());
        assertEquals(1, message.getFields().get(1));
        assertEquals("1", message.getFields().get(2));
    }

    @Test
    public void testDeleteMessage() throws Exception {
        DeleteMessage message = new DeleteMessage("testTable");
        message.addField(1, 1).addField(2, "1");

        byte[] bytes = serializer.serialize(new Request(message));

        Request request = serializer.deserialize(bytes, Request.class);

        assertEquals(2, request.getRequestId());
        assertTrue(request.getMessage() instanceof DeleteMessage);
        assertEquals("testTable", message.getTableName());

        message = (DeleteMessage) request.getMessage();

        assertEquals("testTable", message.getTableName());
        assertEquals(2, message.getFields().size());
        assertEquals(1, message.getFields().get(1));
        assertEquals("1", message.getFields().get(2));
    }

    @Test
    public void testSelectMessage() throws Exception {
        SelectMessage message = new SelectMessage("testTable", 4, "vasa");

        byte[] bytes = serializer.serialize(new Request(message));

        Request request = serializer.deserialize(bytes, Request.class);

        assertEquals(5, request.getRequestId());
        message = (SelectMessage) request.getMessage();

        assertEquals("testTable", message.getTableName());
        assertEquals(4, message.getFieldNumber());
        assertEquals("vasa", message.getFieldValue());

    }

    @Test
    public void testResponseWithoutResult() throws Exception {
        Response response = new Response(1, Response.Status.ERROR);

        byte[] bytes = serializer.serialize(response);

        response = serializer.deserialize(bytes, Response.class);

        assertEquals(1, response.getResponseId());
        assertEquals(Response.Status.ERROR, response.getStatus());
        assertFalse(response.hasNext());
    }

    @Test
    public void testResponse() throws Exception {


        List<Response.Row> rows = new ArrayList<>();
        rows.add(new Response.Row(new Comparable[]{1, 5l, "test"}));
        rows.add(new Response.Row(new Comparable[]{2, 6l, "test2"}));

        Response response = new Response(1, rows);

        byte[] bytes = serializer.serialize(response);

        response = serializer.deserialize(bytes, Response.class);

        assertEquals(1, response.getResponseId());
        assertEquals(Response.Status.OK, response.getStatus());

        assertTrue(response.hasNext());
        Response.Row row = response.next();
        assertEquals(1, row.getInt(0));
        assertEquals(5l, row.getLong(1));
        assertEquals("test", row.getString(2));

        assertTrue(response.hasNext());
        row = response.next();
        assertEquals(2, row.getInt(0));
        assertEquals(6l, row.getLong(1));
        assertEquals("test2", row.getString(2));

        assertFalse(response.hasNext());


    }
}
