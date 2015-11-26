package com.simpledb.api.messages;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yvladimirov on 10/26/15.
 */
public class DeleteMessage extends Message {
    private Map<Integer, Comparable> fields = new HashMap<>();


    public DeleteMessage(String tableName) {
        super(tableName);
    }

    public DeleteMessage addField(int fieldNumber, Comparable value) {
        fields.put(fieldNumber, value);
        return this;
    }

    public Map<Integer, Comparable> getFields() {
        return fields;
    }
}
