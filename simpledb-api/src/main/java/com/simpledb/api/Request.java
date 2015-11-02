package com.simpledb.api;

import com.simpledb.api.messages.Message;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yvladimirov on 10/29/15.
 */
public class Request {
    private static final AtomicInteger requestSeq = new AtomicInteger();
    private int requestId;
    private Message message;

    public Request() {
    }

    public Request(Message message) {
        this.requestId = requestSeq.incrementAndGet();
        this.message = message;
    }

    public int getRequestId() {
        return requestId;
    }

    public Message getMessage() {
        return message;
    }
}
