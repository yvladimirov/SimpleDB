package com.simpledb.server.table;

import org.simpledb.core.builder.TableBuilder;
import org.simpledb.core.table.Table;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yvladimirov on 10/21/15.
 */
public class TableManager {
    private Map<String, Table> tables = new ConcurrentHashMap<>();


    public boolean createTable(TableBuilder tableBuilder) {
        Table table = tableBuilder.build();
        return tables.putIfAbsent(table.getName(), table) == null;
    }


    public Table getTable(String name) {
        return tables.get(name);
    }


    public void dropTable(String name) {
        tables.remove(name).drop();
    }
}
