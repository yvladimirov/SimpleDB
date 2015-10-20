package org.simpledb.core.table;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.simpledb.core.builder.FieldBuilder;
import org.simpledb.core.builder.TableBuilder;

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
    Table tableLongAndStringWithoutIndex;
    Table tableLongAndString;
    Table tableBig;
    long counter;
    Map<Long, Long> map;

    @Setup(Level.Iteration)
    public void prepare() {
        tableLongWithoutIndex = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("test_long_field").type(FieldType.LONG).build())
                .build();
        tableLong = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("test_long_field").type(FieldType.LONG).indexing(true).build())
                .build();
        tableStringWithoutIndex = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("test_string_field").type(FieldType.STRING).build())
                .build();
        tableString = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("test_string_field").type(FieldType.STRING).indexing(true).build())
                .build();
        tableLongAndStringWithoutIndex = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("test_long_field").type(FieldType.LONG).build())
                .addField(FieldBuilder.builder().name("test_string_field").type(FieldType.STRING).build())
                .build();
        tableLongAndString = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("test_long_field").type(FieldType.LONG).indexing(true).build())
                .addField(FieldBuilder.builder().name("test_string_field").type(FieldType.STRING).indexing(true).build())
                .build();
        tableBig = TableBuilder.builder().name("test")
                .addField(FieldBuilder.builder().name("test_long_field").type(FieldType.LONG).indexing(true).build())
                .addField(FieldBuilder.builder().name("test_long_field1").type(FieldType.LONG).build())
                .addField(FieldBuilder.builder().name("test_long_field2").type(FieldType.LONG).build())
                .addField(FieldBuilder.builder().name("test_string_field").type(FieldType.STRING).indexing(true).build())
                .addField(FieldBuilder.builder().name("test_string_field1").type(FieldType.STRING).build())
                .addField(FieldBuilder.builder().name("test_string_field2").type(FieldType.STRING).build())
                .addField(FieldBuilder.builder().name("test_string_field3").type(FieldType.STRING).build())
                .addField(FieldBuilder.builder().name("test_long_field3").type(FieldType.LONG).build())
                .addField(FieldBuilder.builder().name("test_long_field4").type(FieldType.LONG).build())
                .addField(FieldBuilder.builder().name("test_string_field4").type(FieldType.STRING).indexing(true).build())
                .build();


        map = new HashMap<>();

    }

    @TearDown(Level.Iteration)
    public void destroy() {
        tableLongWithoutIndex.drop();
        tableLong.drop();
        tableStringWithoutIndex.drop();
        tableString.drop();
        tableLongAndStringWithoutIndex.drop();
        tableLongAndString.drop();
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
        tableLongAndStringWithoutIndex.add(counter, "" + counter++);
    }

    @Benchmark
    public void tableStringAndLong() {
        tableLongAndString.add(counter, "" + counter++);
    }

    @Benchmark
    public void tableBig() {
        counter++;
        tableBig.add(counter, counter, counter, "" + counter, "" + counter, "" + counter, "" + counter, counter, counter, "" + counter);
    }


    @Benchmark
    public void hashMap() {
        counter++;
        map.put(counter, counter);
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(JMHTablePut.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
