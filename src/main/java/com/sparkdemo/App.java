package com.sparkdemo;

import org.apache.spark.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.BucketingUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class App 
{
    public static final StructType SCHEMA = new StructType()
            .add("Metric", DataTypes.StringType)
            .add("Value", DataTypes.DoubleType)
            .add("Timestamp", DataTypes.TimestampType);

    public static void main(String[] args )
    {
        Properties p = Util.loadProperties();
        SparkSession spark = SparkSession.builder()
                .appName(p.getProperty("sparkContext.app.name"))
                .master(String.format("local[%s]", p.getProperty("sparkContext.nthreads")))
                .getOrCreate();

        //load csv data using predefined schema
        Dataset<Row> df = spark.read()
                .format("csv")
                .schema(SCHEMA)
                .load(p.getProperty("paths.input"));

        Dataset<Row> finalDf = computeAggregations(df,
                Integer.valueOf(p.getProperty("window.duration", "24")),
                p.getProperty("window.time.unit", "hour"));

        finalDf.write()
                .mode(SaveMode.Overwrite)
                .csv(p.getProperty("paths.output"));
        spark.stop();
    }

    /**
     * Computes aggregations Average,Min,Max for each (time bucket, metric) group
     *
     * @param df - input data frame with cols (Timestamp,Metric,Value)
     * @param windowDurationNb - number of time units in the ouput window
     * @param windowTimeUnit - time unit can be week, day, hour, minute, second, millisecond, microsecond
     * @return dataframe with columns (TimeBucketStart, TimeBucketEnd, Metric, AverageValue, MinValue, MaxValue)
     */
    public static Dataset<Row> computeAggregations(@NotNull Dataset<Row> df, Integer windowDurationNb, String windowTimeUnit) {
        String windowDuration = windowDurationNb + " " + windowTimeUnit;
        Dataset<Row> outDf = df.groupBy(functions.window(df.col("Timestamp"), windowDuration).alias("TimeBucket"),
                                         df.col("Metric"))
                                .agg(functions.avg(df.col("Value")).alias("AverageValue"),
                                    functions.min(df.col("Value")).alias("MinValue"),
                                    functions.max(df.col("Value")).alias("MaxValue"));

        // need extra select to extract fields from TimeBucket struct
        return outDf.select(outDf.col("TimeBucket.start").alias("TimeBucketStart"),
                outDf.col("TimeBucket.end").alias("TimeBucketEnd"),
                outDf.col("Metric"),
                outDf.col("AverageValue"),
                outDf.col("MinValue"),
                outDf.col("MaxValue"));
    }

}
