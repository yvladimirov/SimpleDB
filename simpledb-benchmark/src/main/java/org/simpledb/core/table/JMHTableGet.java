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
