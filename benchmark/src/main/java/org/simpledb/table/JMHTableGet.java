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
public class JMHTableGet {
    Map<Long, Long> map;
    Table tableLongWithoutIndex;
    Table tableLong;
    Table tableStringWithoutIndex;
    Table tableString;
    Table tableLongAndStringWithoutIndex;
    Table tableLongAndString;
    Table tableBig;

    @Setup(Level.Trial)
    public void prepare() {
        map = new HashMap<>();
        tableLongWithoutIndex = new Table(new Field[]{new Field(FieldType.LONG, "test_long_field", false)});
        tableLong = new Table(new Field[]{new Field(FieldType.LONG, "test_long_field", true)});
        tableStringWithoutIndex = new Table(new Field[]{new Field(FieldType.STRING, "test_string_field", false)});
        tableString = new Table(new Field[]{new Field(FieldType.STRING, "test_string_field", true)});
        tableLongAndStringWithoutIndex = new Table(new Field[]{new Field(FieldType.LONG, "test_long_field", false), new Field(FieldType.STRING, "test_string_field", false)});
        tableLongAndString = new Table(new Field[]{new Field(FieldType.LONG, "test_long_field", true), new Field(FieldType.STRING, "test_string_field", true)});
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
        for (long i = 0; i < 1000 * 1000; i++) {
            map.put(i, i);
            tableLongWithoutIndex.add(i);
            tableLong.add(i);
            tableStringWithoutIndex.add("" + i);
            tableString.add("" + i);
            tableLongAndString.add(i, "" + i);
            tableLongAndStringWithoutIndex.add(i, "" + i);

        }

    }

    @TearDown(Level.Trial)
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
    public Object[] tableLongWithoutIndex() {
        return tableLongWithoutIndex.findOne(0, 100000l);
    }

    @Benchmark
    public Object[] tableLong() {
        return tableLong.findOne(0, 100000l);
//        return tableLong.getIndexes().get(0).get(100000l);
    }

    @Benchmark
    public Object[] tableStringWithoutIndex() {
        return tableStringWithoutIndex.findOne(0, "100000");
    }

    @Benchmark
    public Object[] tableString() {
        return tableString.findOne(0, "100000");
    }


    @Benchmark
    public Object[] tableLongAndStringByLong() {
        return tableLongAndString.findOne(0, 100000l);
    }

    @Benchmark
    public Object[] tableLongAndStringByString() {
        return tableLongAndString.findOne(1, "100000");
    }

    @Benchmark
    public Long hashMap() {
        return map.get(100000l);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(JMHTableGet.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
