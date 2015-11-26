package com.simpledb.api.messages;

/**
 * Created by yvladimirov on 11/4/15.
 */
public class SelectMessage extends Message {
    private final int fieldNumber;
    private final Comparable fieldValue;

    public SelectMessage(String tableName, int fieldNumber, Comparable fieldValue) {
        super(tableName);
        this.fieldNumber = fieldNumber;
        this.fieldValue = fieldValue;
    }

    public int getFieldNumber() {
        return fieldNumber;
    }

    public Comparable getFieldValue() {
        return fieldValue;
    }
}
