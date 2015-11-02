package com.simpledb.client.network;

/**
 * Created by yvladimirov on 10/29/15.
 */
public interface TCPConnectionListener {

    public void doResponse(byte[] bytes);
}
