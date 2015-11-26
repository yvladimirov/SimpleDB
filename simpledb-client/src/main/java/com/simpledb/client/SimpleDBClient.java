package com.simpledb.client;

import com.simpledb.api.Request;
import com.simpledb.api.Response;
import com.simpledb.api.messages.CreateTableMessage;
import com.simpledb.api.messages.DeleteMessage;
import com.simpledb.api.messages.InsertMessage;
import com.simpledb.api.messages.SelectMessage;
import com.simpledb.client.network.TCPConnectionManager;

import java.io.IOException;

/**
 * Created by yvladimirov on 10/20/15.
 */
public class SimpleDBClient {

    private final TCPConnectionManager connectionManager;

    public SimpleDBClient(String host, int port) throws IOException {
        this.connectionManager = new TCPConnectionManager(host, port);
    }


    public Response createTable(CreateTableMessage message) throws Exception {
        return connectionManager.send(new Request(message));
    }

    public Response dropTable(DeleteMessage message) throws Exception {
        return connectionManager.send(new Request(message));
    }

    public Response insert(InsertMessage message) throws Exception {
        return connectionManager.send(new Request(message));
    }

    public Response delete(DeleteMessage message) throws Exception {
        return connectionManager.send(new Request(message));
    }

    public Response select(SelectMessage message) throws Exception {
        return connectionManager.send(new Request(message));
    }


    public void close() throws IOException {
        connectionManager.close();
    }
}
