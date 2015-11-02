package com.simpledb.client.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

/**
 * Created by yvladimirov on 10/19/15.
 */
public class TCPConnection extends Thread {
    private static Logger logger = LoggerFactory.getLogger(TCPConnection.class);
    private final ByteBuffer buffer = ByteBuffer.allocate(8192);
    private final Queue<byte[]> queue = new ConcurrentLinkedQueue<>();
    private List<TCPConnectionListener> listeners = new CopyOnWriteArrayList<>();
    private final SocketChannel channel;
    private final Selector selector;

    public TCPConnection(String host, int port) throws IOException {
        logger.info("Connect to {}:{}", host, port);

        selector = SelectorProvider.provider().openSelector();
        channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.connect(new InetSocketAddress(host, port));
        channel.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
        start();
    }

    @Override
    public void run() {
        while (!interrupted()) {
            try {
                for (byte[] element; (element = queue.poll()) != null; ) {
                    write(element);
                }

                int n = selector.select();
                if (n == 0) {
                    continue;
                }

                Set<SelectionKey> keys = selector.selectedKeys();
                for (SelectionKey key : keys) {
                    if (!key.isValid()) {
                        continue;
                    }
                    if (key.isConnectable()) {
                        connection(key);
                    } else if (key.isReadable()) {
                        read(key);
                    } else if (key.isWritable()) {
//                        write(key);
                    }
                }
                keys.clear();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private void read(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        buffer.clear();
        try {
            int numRead = socketChannel.read(this.buffer);
            if (numRead == -1) {
                key.channel().close();
                key.cancel();
            } else {
                buffer.flip();
                byte[] bytes = new byte[numRead];
                System.arraycopy(buffer.array(), 0, bytes, 0, numRead);
                listeners.stream().forEach(listener -> listener.doResponse(bytes));
            }
        } catch (IOException e) {
            key.cancel();
            socketChannel.close();
        }
    }

    private void write(byte[] bytes) throws IOException {
        buffer.clear();
        buffer.put(bytes);
        buffer.flip();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private void connection(SelectionKey key) {
        try {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            socketChannel.finishConnect();
            key.interestOps(SelectionKey.OP_READ);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            key.cancel();
        }
    }


    public void send(byte[] bytes) throws ExecutionException, InterruptedException {
        queue.offer(bytes);
        selector.wakeup();
    }

    public void close() throws IOException {
        selector.close();
        channel.close();
    }

    public void addListener(TCPConnectionListener listener) {
        this.listeners.add(listener);
    }
}
