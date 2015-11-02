package com.simpledb.api.messages;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yvladimirov on 10/26/15.
 */
public class CreateTableMessage extends Message {
    private List<Field> fields = new ArrayList<>();

    public CreateTableMessage(String tableName) {
        super(tableName);
    }

    public CreateTableMessage addField(String fieldName, FieldType type, boolean isIndexing) {
        fields.add(new Field(fieldName, type, isIndexing));
        return this;
    }

    public List<Field> getFields() {
        return fields;
    }

    public static class Field {

        private final String fieldName;
        private final FieldType type;
        private final boolean isIndexing;

        public Field(String fieldName, FieldType type, boolean isIndexing) {
            this.fieldName = fieldName;
            this.type = type;
            this.isIndexing = isIndexing;
        }

        public String getFieldName() {
            return fieldName;
        }

        public FieldType getType() {
            return type;
        }

        public boolean isIndexing() {
            return isIndexing;
        }
    }

    public enum FieldType {
        STRING, INTEGER, LONG
    }
}
