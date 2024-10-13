package com.sparkdemo;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class AppTest {
    SparkSession spark;

    @Before
    public void init() {
        spark = SparkSession.builder()
                .appName("Batch Aggregation")
                .master("local[1]")
                .getOrCreate();
    }

    @Test
    public void test24HourAggregation() {
        List<Row> rows = Arrays.asList(
                RowFactory.create("metric1",  10.0, Instant.parse("2024-01-02T21:42:28.000Z")),
                RowFactory.create("metric2", 20.0,  Instant.parse("2024-01-02T12:42:28.000Z")),
                RowFactory.create("metric1", 30.0, Instant.parse("2024-01-02T01:42:28.000Z")),
                RowFactory.create("metric2", 40.0,  Instant.parse("2024-01-02T01:42:28.000Z"))
        );
        Dataset<Row> df = spark.createDataFrame(rows, App.SCHEMA);
        
        Dataset<Row> aggDf = App.computeAggregations(df, 24, "hour");
        aggDf.printSchema();

        List<Row> result = aggDf.collectAsList();
        result.sort(Comparator.comparing(row -> row.getString(row.fieldIndex("Metric"))));
        List<String> resultStrings = result.stream().map(row -> row.toString()).collect(Collectors.toList());

        List<String> expectedResults = Arrays.asList(
                "[2024-01-02 00:00:00.0,2024-01-03 00:00:00.0,metric1,20.0,10.0,30.0]",
                "[2024-01-02 00:00:00.0,2024-01-03 00:00:00.0,metric2,30.0,20.0,40.0]"
        );

        assertThat(resultStrings, is(expectedResults));
    }

    @Test
    public void test6HourAggregation() {
        List<Row> rows = Arrays.asList(
                RowFactory.create("metric1",  10.0, Instant.parse("2024-01-02T03:42:28.000Z")),
                RowFactory.create("metric1", 30.0, Instant.parse("2024-01-02T05:42:28.000Z")),
                RowFactory.create("metric1", 50.0, Instant.parse("2024-01-02T08:42:28.000Z")),

                RowFactory.create("metric2", 20.0,  Instant.parse("2024-01-02T05:42:28.000Z")),
                RowFactory.create("metric2", 40.0,  Instant.parse("2024-01-02T14:42:28.000Z")),
                RowFactory.create("metric2", 60.0,  Instant.parse("2024-01-02T15:42:28.000Z"))
        );
        Dataset<Row> df = spark.createDataFrame(rows, App.SCHEMA);

        Dataset<Row> aggDf = App.computeAggregations(df, 6, "hour");
        //aggDf.show();

        List<Row> result = aggDf.collectAsList();
        result.sort(Comparator.comparing(row -> row.getTimestamp(0) + row.getString(row.fieldIndex("Metric"))));
        List<String> resultStrings = result.stream().map(Row::toString).collect(Collectors.toList());

        List<String> expectedResults = Arrays.asList(
                "[2024-01-02 00:00:00.0,2024-01-02 06:00:00.0,metric1,20.0,10.0,30.0]",
                "[2024-01-02 00:00:00.0,2024-01-02 06:00:00.0,metric2,20.0,20.0,20.0]",
                "[2024-01-02 06:00:00.0,2024-01-02 12:00:00.0,metric1,50.0,50.0,50.0]",
                "[2024-01-02 12:00:00.0,2024-01-02 18:00:00.0,metric2,50.0,40.0,60.0]"
        );

        assertThat(resultStrings, is(expectedResults));
    }

    @Test
    public void test30MinAggregation() {
        List<Row> rows = Arrays.asList(
                RowFactory.create("metric1",  10.0, Instant.parse("2024-01-02T03:12:28.000Z")),
                RowFactory.create("metric1", 30.0, Instant.parse("2024-01-02T03:22:28.000Z")),
                RowFactory.create("metric1", 50.0, Instant.parse("2024-01-02T03:42:28.000Z")),

                RowFactory.create("metric2", 20.0,  Instant.parse("2024-01-02T03:12:28.000Z")),
                RowFactory.create("metric2", 40.0,  Instant.parse("2024-01-02T04:12:28.000Z")),
                RowFactory.create("metric2", 60.0,  Instant.parse("2024-01-02T04:25:28.000Z"))
        );
        Dataset<Row> df = spark.createDataFrame(rows, App.SCHEMA);

        Dataset<Row> aggDf = App.computeAggregations(df, 30, "minute");
        //aggDf.show();

        List<Row> result = aggDf.collectAsList();
        result.sort(Comparator.comparing(row -> row.getTimestamp(0) + row.getString(row.fieldIndex("Metric"))));
        List<String> resultStrings = result.stream().map(Row::toString).collect(Collectors.toList());

        List<String> expectedResults = Arrays.asList(
                "[2024-01-02 03:00:00.0,2024-01-02 03:30:00.0,metric1,20.0,10.0,30.0]",
                "[2024-01-02 03:00:00.0,2024-01-02 03:30:00.0,metric2,20.0,20.0,20.0]",
                "[2024-01-02 03:30:00.0,2024-01-02 04:00:00.0,metric1,50.0,50.0,50.0]",
                "[2024-01-02 04:00:00.0,2024-01-02 04:30:00.0,metric2,50.0,40.0,60.0]"
        );

        assertThat(resultStrings, is(expectedResults));
    }
}
