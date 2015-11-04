package com.simpledb.server;

import com.simpledb.api.Request;
import com.simpledb.api.Response;
import com.simpledb.api.messages.CreateTableMessage;
import com.simpledb.api.messages.InsertMessage;
import com.simpledb.api.serializer.Serializer;
import com.simpledb.server.table.TableManager;
import org.simpledb.core.builder.TableBuilder;
import org.simpledb.core.table.Field;
import org.simpledb.core.table.FieldType;
import org.simpledb.core.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yvladimirov on 10/22/15.
 */
public class Dispatcher {
    private static Logger logger = LoggerFactory.getLogger(Dispatcher.class);
    private final TableManager tableManager = new TableManager();
    private Serializer serializer = new Serializer();
    private ByteBuffer byteBuffer = ByteBuffer.allocate(512);

    private AtomicInteger counter = new AtomicInteger();
    private long times = System.currentTimeMillis();


    public void handle(SocketChannel channel, byte[] bytes) {
        try {
//            logger.info("{}",counter.get());
            if(counter.incrementAndGet()%100000==0){
                logger.info("100000 transaction by {}", System.currentTimeMillis()-times);
                times = System.currentTimeMillis();
            }

            Request request = serializer.deserialize(bytes, Request.class);

            if (request.getMessage() instanceof CreateTableMessage) {
                handleCreateTable((CreateTableMessage)request.getMessage());
                writeResponse(channel, new Response(request.getRequestId(), null));
            } else if (request.getMessage() instanceof InsertMessage) {
                handleInsertTable((InsertMessage)request.getMessage());
                writeResponse(channel, new Response(request.getRequestId(), null));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void handleInsertTable(InsertMessage message) throws IOException {

        Table table = tableManager.getTable(message.getTableName());
        table.insert(message.getFields().values().toArray(new Comparable[message.getFields().size()]));
    }

    private void handleCreateTable(CreateTableMessage message) {
        TableBuilder builder = TableBuilder.builder();
        builder.name(message.getTableName());
        for(CreateTableMessage.Field field:message.getFields()){
            builder.addField(new Field(FieldType.valueOf(field.getType().name()), field.getFieldName(), field.isIndexing()));
        }
        tableManager.createTable(builder);
    }

    private void writeResponse(SocketChannel channel, Response response) throws IOException {
        byteBuffer.clear();
        byteBuffer.put(serializer.serialize(response));
        byteBuffer.flip();
        while (byteBuffer.hasRemaining()) {
            channel.write(byteBuffer);
        }
    }
}
