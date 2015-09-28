package org.simpledb.table;

/**
 * Created by yvladimirov on 9/25/15.
 */
public class Field {
    private final FieldType type;
    private final String name;
    private final boolean indexing;

    public Field(FieldType type, String name, boolean indexing) {
        this.type = type;
        this.name = name;
        this.indexing = indexing;
    }


    public void write(long address, Object value) {
        type.getWriter().write(address, value);
    }

    public long getSize() {
        return type.getSize();
    }

    public Object read(long address) {
        return type.getReader().read(address);
    }

    public boolean isIndexing() {
        return indexing;
    }

    public Reader getReader() {
        return type.getReader();
    }
}
