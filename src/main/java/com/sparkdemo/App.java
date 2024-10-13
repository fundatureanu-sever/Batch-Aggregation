package com.sparkdemo;

import org.apache.spark.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.BucketingUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions.*;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession.builder()
                .appName("Batch Aggregation")
                .master("local[2]")
                .getOrCreate();

        StructType schema = new StructType().add("Metric", DataTypes.StringType)
                .add("Value", DataTypes.DoubleType)
                .add("Timestamp", DataTypes.TimestampType);
        Dataset<Row> df = spark.read().format("csv").schema(schema).load("input.csv");

        Dataset<Row> outDf = df.groupBy(functions.window(df.col("Timestamp"), "4 hour").alias("TimeBucket"),
                                         df.col("Metric"))
                                .agg(functions.avg(df.col("Value")).alias("AverageValue"),
                                    functions.min(df.col("Value")).alias("MinValue"),
                                    functions.max(df.col("Value")).alias("MaxValue"));

        Dataset<Row> finalDf = outDf.select(outDf.col("TimeBucket.start").alias("TimeBucketStart"),
                outDf.col("TimeBucket.end").alias("TimeBucketEnd"),
                outDf.col("Metric"),
                outDf.col("AverageValue"),
                outDf.col("MinValue"),
                outDf.col("MaxValue"));
        finalDf.show();

        finalDf.write().mode(SaveMode.Overwrite).csv("output");

    }

}
