package com.simpledb.api.messages;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yvladimirov on 10/26/15.
 */
public class DeleteMessage extends Message {
    private Map<String, Comparable> fields = new HashMap<>();


    public DeleteMessage(String tableName) {
        super(tableName);
    }

    public DeleteMessage addField(String name, Comparable value) {
        fields.put(name, value);
        return this;
    }

    public Map<String, Comparable> getFields() {
        return fields;
    }
}
