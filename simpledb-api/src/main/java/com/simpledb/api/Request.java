package com.simpledb.api;

import com.simpledb.api.messages.Message;

/**
 * Created by yvladimirov on 10/29/15.
 */
public class Request {
    private int requestId;
    private Message message;

    public Request() {
    }

    public Request(int requestId, Message message) {
        this.requestId = requestId;
        this.message = message;
    }

    public int getRequestId() {
        return requestId;
    }

    public Message getMessage() {
        return message;
    }
}
