package com.simpledb.api.messages;

/**
 * Created by yvladimirov on 10/26/15.
 */
public class DropTableMessage extends Message {

    public DropTableMessage(String tableName) {
        super(tableName);
    }
}
