package com.simpledb.api.messages;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yvladimirov on 10/26/15.
 */
public class InsertMessage extends Message {
    private Map<Integer, Comparable> fields = new HashMap<>();

    public InsertMessage(String tableName) {
        super(tableName);
    }

    public InsertMessage addField(int fieldNumber, Comparable value) {
        fields.put(fieldNumber, value);
        return this;
    }

    public Map<Integer, Comparable> getFields() {
        return fields;
    }
}
