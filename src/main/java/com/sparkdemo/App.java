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
    public static final StructType SCHEMA = new StructType()
            .add("Metric", DataTypes.StringType)
            .add("Value", DataTypes.DoubleType)
            .add("Timestamp", DataTypes.TimestampType);

    public static void main(String[] args )
    {
        SparkSession spark = SparkSession.builder()
                .appName("Batch Aggregation")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv").schema(SCHEMA).load("input");

        Dataset<Row> finalDf = computeAggregations(df, 4, "hour");

        finalDf.write().mode(SaveMode.Overwrite).csv("output");

    }

    public static Dataset<Row> computeAggregations(Dataset<Row> df, Integer windowDurationNb, String windowTimeUnit) {
        String windowDuration = windowDurationNb + " " + windowTimeUnit;
        Dataset<Row> outDf = df.groupBy(functions.window(df.col("Timestamp"), windowDuration).alias("TimeBucket"),
                                         df.col("Metric"))
                                .agg(functions.avg(df.col("Value")).alias("AverageValue"),
                                    functions.min(df.col("Value")).alias("MinValue"),
                                    functions.max(df.col("Value")).alias("MaxValue"));

        return outDf.select(outDf.col("TimeBucket.start").alias("TimeBucketStart"),
                outDf.col("TimeBucket.end").alias("TimeBucketEnd"),
                outDf.col("Metric"),
                outDf.col("AverageValue"),
                outDf.col("MinValue"),
                outDf.col("MaxValue"));
    }

}
