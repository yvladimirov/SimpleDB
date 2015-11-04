package com.simpledb.server;

import com.simpledb.server.network.SimpleDBServer;

/**
 * Created by yvladimirov on 10/22/15.
 */
public class SimpleDBStarter {
    public static void main(String[] args) throws Exception {
        SimpleDBServer server = new SimpleDBServer(9090);
        server.start();

        server.join();
        server.close();
    }
}
