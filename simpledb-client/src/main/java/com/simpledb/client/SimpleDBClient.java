package com.simpledb.client;

import com.simpledb.api.Request;
import com.simpledb.api.Response;
import com.simpledb.api.messages.CreateTableMessage;
import com.simpledb.api.messages.DeleteMessage;
import com.simpledb.api.messages.InsertMessage;
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


    public void createTable(CreateTableMessage message) throws Exception {
        Response response = connectionManager.send(new Request(message));
    }

    public void dropTable(DeleteMessage message) throws Exception {
        connectionManager.send(new Request(message));
    }

    public void insert(InsertMessage message) throws Exception {
        connectionManager.send(new Request(message));
    }

    public void delete(DeleteMessage message) throws Exception {
        connectionManager.send(new Request(message));
    }


    public void close() throws IOException {
        connectionManager.close();
    }
}
