package org.simpledb.table;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yvladimirov on 9/27/15.
 */
@State(Scope.Benchmark)
public class JMHTablePut {
    Table tableLongWithoutIndex;
    Table tableLong;
    Table tableStringWithoutIndex;
    Table tableString;
    Table tableStringAndLongWithoutIndex;
    Table tableStringAndLong;
    Table tableBig;
    long counter;
    Map<Long,Long> map;

    @Setup(Level.Iteration)
    public void prepare() {
        tableLongWithoutIndex = new Table(new Field[]{new Field(FieldType.LONG, "test_long_field", false)});
        tableLong = new Table(new Field[]{new Field(FieldType.LONG, "test_long_field", true)});
        tableStringWithoutIndex = new Table(new Field[]{new Field(FieldType.STRING, "test_string_field", false)});
        tableString = new Table(new Field[]{new Field(FieldType.STRING, "test_string_field", true)});
        tableStringAndLongWithoutIndex = new Table(new Field[]{new Field(FieldType.LONG, "test_long_field", false), new Field(FieldType.STRING, "test_string_field", false)});
        tableStringAndLong = new Table(new Field[]{new Field(FieldType.LONG, "test_long_field", true), new Field(FieldType.STRING, "test_string_field", true)});
        tableBig = new Table(new Field[]{
                new Field(FieldType.LONG, "test_long_field", true),
                new Field(FieldType.LONG, "test_long_field1", false),
                new Field(FieldType.LONG, "test_long_field2", false),
                new Field(FieldType.STRING, "test_string_field", true),
                new Field(FieldType.STRING, "test_string_field1", false),
                new Field(FieldType.STRING, "test_string_field2", false),
                new Field(FieldType.STRING, "test_string_field3", false),
                new Field(FieldType.LONG, "test_long_field3", false),
                new Field(FieldType.LONG, "test_long_field4", false),
                new Field(FieldType.STRING, "test_string_field4", true)
        });

        map = new HashMap<>();

    }

    @TearDown(Level.Iteration)
    public void destroy() {
        tableLongWithoutIndex.drop();
        tableLong.drop();
        tableStringWithoutIndex.drop();
        tableString.drop();
        tableStringAndLongWithoutIndex.drop();
        tableStringAndLong.drop();
        tableBig.drop();
    }


    @Benchmark
    public void tableLongWithoutIndex() {
        tableLongWithoutIndex.add(counter++);
    }

    @Benchmark
    public void tableLong() {
        tableLong.add(counter++);
    }

    @Benchmark
    public void tableStringWithoutIndex() {
        tableStringWithoutIndex.add("" + counter++);
    }

    @Benchmark
    public void tableString() {
        tableString.add("" + counter++);
    }

    @Benchmark
    public void tableStringAndLongWithoutIndex() {
        tableStringAndLongWithoutIndex.add(counter, "" + counter++);
    }

    @Benchmark
    public void tableStringAndLong() {
        tableStringAndLong.add(counter, "" + counter++);
    }

    @Benchmark
    public void tableBig() {
        counter++;
        tableBig.add(counter, counter, counter, "" + counter, "" + counter, "" + counter, "" + counter, counter, counter, "" + counter);
    }


    @Benchmark
    public void hashMap(){
        counter++;
        map.put(counter,counter);
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(JMHTablePut.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
