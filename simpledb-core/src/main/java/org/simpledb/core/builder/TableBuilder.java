package org.simpledb.core.builder;

import com.google.common.collect.Lists;
import org.simpledb.core.table.Field;
import org.simpledb.core.table.Table;

import java.util.List;

/**
 * Created by yvladimirov on 9/30/15.
 */
public class TableBuilder {
    private List<Field> fields = Lists.newArrayList();
    private String name;

    private TableBuilder() {
    }

    public static TableBuilder builder() {
        return new TableBuilder();
    }

    public TableBuilder name(String name) {
        this.name = name;
        return this;
    }

    public TableBuilder addField(Field field) {
        this.fields.add(field);
        return this;
    }


    public Table build() {
        return new Table(name, fields.toArray(new Field[0]));
    }
}
