package org.simpledb.core.table;

/**
 * Created by yvladimirov on 10/22/15.
 */
public interface Table {

    public String getName();

    public void insert(Comparable... values);


    public Comparable[] findOne(int fieldNumber, Comparable value);

    public Comparable[] delete(int fieldNumber, Comparable value);


    public void update(int fieldNumber, Comparable value, Comparable... values);

    public void drop();
}
