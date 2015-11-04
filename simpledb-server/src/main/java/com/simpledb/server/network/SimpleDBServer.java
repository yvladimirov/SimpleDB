package com.simpledb.server.network;

import com.simpledb.server.Dispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

/**
 * Created by yvladimirov on 10/22/15.
 */
public class SimpleDBServer extends Thread {
    private final Logger logger = LoggerFactory.getLogger(SimpleDBServer.class);
    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private ByteBuffer buffer = ByteBuffer.allocate(1024);
    private Dispatcher dispatcher = new Dispatcher();

    public SimpleDBServer(int port) throws IOException {
        super("SimpleDBServer");
        logger.info("Start SimpleDB port:{}", port);
        serverChannel = ServerSocketChannel.open();
        ServerSocket serverSocket = serverChannel.socket();
        selector = Selector.open();
        serverSocket.bind(new InetSocketAddress(port));
        serverChannel.configureBlocking(false);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    @Override
    public void run() {
        while (true) {
            try {
                int n = selector.select();

                if (n == 0) {
                    continue;
                }

                Set<SelectionKey> keys = selector.selectedKeys();

                for (SelectionKey key : keys) {

                    if (key.isReadable()) {
                        readDataFromSocket(key);
                    } else if (key.isAcceptable()) {
                        registerChannel(key);
                    }
                }
                keys.clear();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public void close() throws IOException {
        selector.close();
        serverChannel.close();
    }

    private void registerChannel(SelectionKey key) throws Exception {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel channel = server.accept();
        channel.configureBlocking(false);
        channel.register(key.selector(), SelectionKey.OP_READ);

        logger.info("Accept connection from:{}", channel.getRemoteAddress());
    }


    private void readDataFromSocket(SelectionKey key) throws Exception {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        int count;
        buffer.clear();

        while ((count = socketChannel.read(buffer)) > 0) {
            buffer.flip();
            byte[] bytes = new byte[count];
            System.arraycopy(buffer.array(), 0, bytes, 0, count);
            dispatcher.handle(socketChannel, bytes);
        }


        if (count < 0) {
            socketChannel.close();
        }
    }
}
