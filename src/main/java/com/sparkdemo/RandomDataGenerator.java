package com.sparkdemo;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class RandomDataGenerator {
    public static void main(String[] args) {
        Properties p = Util.loadProperties();
        SparkSession spark = SparkSession.builder()
                .appName("Random Data Generator")
                .master("local[1]")
                .getOrCreate();

        List<String> values = Arrays.asList("precipitation", "temperature", "pressure", "air moisture", "windspeed");
        Random random = new Random();
        long startEpoch = LocalDateTime.of(2024, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC);
        long endEpoch = LocalDateTime.of(2024, 1, 3, 23, 59).toEpochSecond(ZoneOffset.UTC);

        // Create 2000 rows of random data
        Row[] rows = new Row[2000];
        for (int i = 0; i < 2000; i++) {
            String column1 = values.get(random.nextInt(values.size()));
            double column2 = 10 + (90 * random.nextDouble()); // Random double between 10 and 100
            long randomEpoch = startEpoch + (long) (random.nextDouble() * (endEpoch - startEpoch));
            Instant instant = Instant.ofEpochSecond(randomEpoch);
            rows[i] = RowFactory.create(column1, column2, instant);
        }

        Dataset<Row> df = spark.createDataFrame(Arrays.asList(rows), App.SCHEMA);

        df.write().mode(SaveMode.Overwrite).csv(p.getProperty("paths.input"));
    }
}

