# Spark Batch Aggregation Demo

## Description
This project computes aggregations for timeseries data. The input dataset contains 3 columns: metric, value and timestamp and is assumed to be in CSV format. 
The batch job aggregates the data by metric and time bucket and writes the results also in CSV format.

For each time bucket and each metric in the bucket, we calculate the average, minimum and maximum value of the metric.
The time bucket can be customized in the provided config file by overriding the `window.duration` and `window.time.unit`. 
In the output dataset we represent the time bucket by 2 columns: the moment at the start of the time bucket and the moment at the end of the bucket.


## Build and run
Prerequisites: Maven + JDK 17 installation

Build: `mvn clean package`

Run: `mvn exec:java -Dexec.mainClass=com.sparkdemo.App`

Alternatively the code can be run using `run.sh` script

For running in Intellij you need to add following VM options: `--add-exports java.base/sun.nio.ch=ALL-UNNAMED`, due to conflict between the Spark version used and JDK 17.

## Code structure
```
 src
  |----main
  |      |---java.com.sparkdemo
  |      |              |--App.java
  |      |              |--RandomDataGenerator.java
  |      |              |--Util.java
  |      |              |--WindowTimeUnit.java
  |      |---resources
  |             |--config.properties
  |----test/java/com/sparkdemo
                        |--AppTest.java
  
```
## Configuration
```
sparkContext.app.name=Batch-Aggregation
sparkContext.nthreads=number of threads for Spark locally

paths.input=folder there data is read from
paths.output=folder where data is written to

window.duration=integer value
window.time.unit=week|day|hour|minute|second|millisecond|microsecond
```

## Input output format
Input dataset contains 3 columns
- Metric: StringType
- Value: DoubleType
- Timestamp: TimestampType

Output format contains 5 columns
- TimeBucketStart: TimestampType
- TimeBucketEnd: TimestampType
- Metric: StringType
- AverageValue: DoubleType
- MinValue: DoubleType
- MaxValue: DoubleType

## Data generation
We generated 2000 rows of random data using the `RandomDataGenerator` and written them to `paths.input` location. 

The command to run the data generator:

`mvn exec:java -Dexec.mainClass=com.sparkdemo.RandomDataGenerator`
