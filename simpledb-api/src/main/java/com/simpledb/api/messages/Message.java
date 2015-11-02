package com.simpledb.api.messages;

/**
 * Created by yvladimirov on 10/23/15.
 */
public abstract class Message {
    private String tableName;

    public Message(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
