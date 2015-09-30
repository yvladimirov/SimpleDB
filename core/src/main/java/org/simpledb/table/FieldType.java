package org.simpledb.table;

import org.simpledb.table.reader.*;
import org.simpledb.table.writer.*;

/**
 * Created by yvladimirov on 9/25/15.
 */
public enum FieldType {
    LONG(new LongWriter(), new LongReader()),
    STRING(new StringWriter(), new StringReader()),
    BOOLEAN(new BooleanWriter(), new BooleanReader()),
    INTEGER(new IntegerWriter(), new IntegerReader()),
    BYTES(new BytesWriter(), new BytesReader());

    private Writer writer;
    private Reader reader;

    FieldType(Writer writer, Reader reader) {
        this.writer = writer;
        this.reader = reader;
    }

    public Writer getWriter() {
        return writer;
    }

    public Reader getReader() {
        return reader;
    }
}
