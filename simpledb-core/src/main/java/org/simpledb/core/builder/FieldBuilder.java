package org.simpledb.core.builder;

import org.simpledb.core.table.FieldType;
import org.simpledb.core.table.Field;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class FieldBuilder {

    private FieldType type;
    private String name;
    private boolean indexing;

    private FieldBuilder() {
    }

    public static FieldBuilder builder() {
        return new FieldBuilder();
    }

    public FieldBuilder name(String name) {
        this.name = name;
        return this;
    }

    public FieldBuilder type(FieldType type) {
        this.type = type;
        return this;
    }

    public FieldBuilder indexing(boolean indexing) {
        this.indexing = indexing;
        return this;
    }


    public Field build() {
        return new Field(type, name, indexing);
    }

}
