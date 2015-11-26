package com.simpledb.api;

import java.util.Iterator;
import java.util.List;

/**
 * Created by yvladimirov on 10/29/15.
 */
public class Response implements Iterator<Response.Row> {
    private int responseId;
    private Status status;
    private List<Row> rows;
    private int currentRow;

    public Response() {
    }

    public Response(int responseId) {
        this(responseId, Status.OK);
    }

    public Response(int responseId, Status status) {
        this.responseId = responseId;
        this.status = status;
    }

    public Response(int responseId, List<Row> rows) {
        this(responseId, Status.OK);
        this.rows = rows;
    }

    public int getResponseId() {
        return responseId;
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public boolean hasNext() {
        return currentRow < (rows != null ? rows.size() : 0);
    }

    @Override
    public Row next() {
        return rows.get(currentRow++);
    }

    public static class Row {
        private Comparable[] row;

        public Row(Comparable[] row) {
            this.row = row;
        }

        public String getString(int fieldNumber) {
            return (String) row[fieldNumber];
        }

        public int getInt(int fieldNumber) {
            return (int) row[fieldNumber];
        }

        public long getLong(int fieldNumber) {
            return (long) row[fieldNumber];
        }

        public boolean getBoolean(int fieldNumber) {
            return (boolean) row[fieldNumber];
        }
    }

    public enum Status {
        OK, ERROR
    }
}
