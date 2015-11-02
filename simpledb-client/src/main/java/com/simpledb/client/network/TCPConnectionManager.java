package com.simpledb.client.network;

import com.google.common.util.concurrent.SettableFuture;
import com.simpledb.api.Request;
import com.simpledb.api.Response;
import com.simpledb.api.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yvladimirov on 10/29/15.
 */
public class TCPConnectionManager implements TCPConnectionListener {
    private static final Logger logger = LoggerFactory.getLogger(TCPConnectionManager.class);
    private final Map<Integer, SettableFuture<Response>> result = new ConcurrentHashMap<>();
    private final Serializer serializer = new Serializer();


    private TCPConnection connection;

    public TCPConnectionManager(String host, int port) throws IOException {
        this.connection = new TCPConnection(host, port);
        this.connection.addListener(this);
    }

    public Response send(Request request) throws Exception {
        SettableFuture<Response> future = SettableFuture.create();
        result.put(request.getRequestId(), future);
        connection.send(serializer.serialize(request));
        return future.get();
    }

    @Override
    public void doResponse(byte[] bytes) {
        try {
            Response response = serializer.deserialize(bytes, Response.class);
            result.remove(response.getResponseId()).set(response);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void close() throws IOException {
        connection.close();
    }
}
