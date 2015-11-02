package com.simpledb.api.messages;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yvladimirov on 10/26/15.
 */
public class InsertMessage extends Message {
    private Map<String, Object> fields = new HashMap<>();

    public InsertMessage(String tableName) {
        super(tableName);
    }

    public InsertMessage addField(String name, Object value) {
        fields.put(name, value);
        return this;
    }

    public Map<String, Object> getFields() {
        return fields;
    }
}
