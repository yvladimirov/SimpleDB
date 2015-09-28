package org.simpledb.table;

/**
 * Created by yvladimirov on 9/25/15.
 */
public enum FieldType {
    LONG(new Writer.LongWriter(), new Reader.LongReader(), 8), STRING(new Writer.StringWriter(), new Reader.StringReader(), 100);

    private Writer writer;
    private Reader reader;
    private int size;

    FieldType(Writer writer, Reader reader, int size) {
        this.writer = writer;
        this.reader = reader;
        this.size = size;
    }

    public int getSize() {
        return size;
    }

    public Writer getWriter() {
        return writer;
    }

    public Reader getReader() {
        return reader;
    }
}
