package com.simpledb.api;

import com.simpledb.api.messages.Message;

/**
 * Created by yvladimirov on 10/29/15.
 */
public class Response {
    private int responseId;
    private Message message;


    public Response() {
    }

    public Response(int responseId, Message message) {
        this.responseId = responseId;
        this.message = message;
    }

    public int getResponseId() {
        return responseId;
    }

    public Message getMessage() {
        return message;
    }
}
