from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofweek, when, col, avg, count, max, sum, unix_timestamp
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYC Taxi Analysis") \
    .getOrCreate()

# Load taxi data from CSV file (assumes CSV has headers)
taxi_df = spark.read.csv("yellow_tripdata_2023-01.csv", header=True, inferSchema=True)

# Add after loading taxi_df
print("Number of records:", taxi_df.count())
print("Schema:")
taxi_df.printSchema()

# Create RDD from DataFrame
taxi_rdd = taxi_df.rdd

# Create sample RDD using parallelize
sample_trip_ids = ["1", "2", "3", "4", "5"]
sample_rdd = spark.sparkContext.parallelize(sample_trip_ids)

# Parse RDD data
def parse_record(row):
    try:
        return (
            row.PULocationID,  # PULocationID as key
            (float(row.fare_amount), float(row.trip_distance))  # (fare_amount, trip_distance) as value
        )
    except:
        return None

parsed_rdd = taxi_rdd.map(parse_record).filter(lambda x: x is not None)

# GroupByKey - group trips by pickup location
grouped_by_pickup = parsed_rdd.groupByKey().mapValues(list)

# ReduceByKey - calculate total fare per pickup location
total_fare_by_pickup = parsed_rdd.mapValues(lambda x: x[0]).reduceByKey(lambda x, y: x + y)

# Load zone lookup data (assuming this is still CSV)
zone_df = spark.read.csv("taxi_zone_lookup.csv", header=True, inferSchema=True)
zone_rdd = zone_df.rdd.map(lambda row: (row.LocationID, row.Borough))

# Join trip data with zone lookup
joined_data = total_fare_by_pickup.join(zone_rdd)

# Sort by fare amount
sorted_by_fare = parsed_rdd.sortBy(lambda x: x[1][0], ascending=False)

# Register temporary view for SQL queries
taxi_df.createOrReplaceTempView("taxi_trips")
zone_df.createOrReplaceTempView("zones")

# SQL query for average fare per borough
avg_fare_query = """
SELECT z.Borough, AVG(t.fare_amount) as avg_fare
FROM taxi_trips t
JOIN zones z ON t.PULocationID = z.LocationID
GROUP BY z.Borough
ORDER BY avg_fare DESC
"""
avg_fare_by_borough = spark.sql(avg_fare_query)

# Add trip duration column
taxi_df_with_duration = taxi_df.withColumn(
    "trip_duration_minutes",
    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
)

# Calculate summary statistics
summary_stats = taxi_df.groupBy("PULocationID").agg(
    count("*").alias("total_trips"),
    avg("trip_distance").alias("avg_distance"),
    max("fare_amount").alias("max_fare"),
    sum("fare_amount").alias("total_fare")
)

# Feature engineering for ML
ml_df = taxi_df.select(
    "fare_amount",
    "trip_distance",
    "PULocationID",
    hour("tpep_pickup_datetime").alias("pickup_hour"),
    dayofweek("tpep_pickup_datetime").alias("pickup_dow")
)

# Prepare features for ML
feature_columns = ["trip_distance", "PULocationID", "pickup_hour", "pickup_dow"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
ml_data = assembler.transform(ml_df)

# Split data into training and testing sets
(training_data, test_data) = ml_data.randomSplit([0.8, 0.2], seed=42)

# Train Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="fare_amount")
model = lr.fit(training_data)

# Make predictions and evaluate
predictions = model.transform(test_data)
evaluator = RegressionEvaluator(
    labelCol="fare_amount",
    predictionCol="prediction",
    metricName="rmse"
)

rmse = evaluator.evaluate(predictions)
r2 = evaluator.setMetricName("r2").evaluate(predictions)

# Print evaluation metrics
print(f"Root Mean Square Error (RMSE): {rmse}")
print(f"R-squared (R2): {r2}")

# Save results (in Parquet format for better performance)
avg_fare_by_borough.write.parquet("results/avg_fare_by_borough")
summary_stats.write.parquet("results/summary_stats")
predictions.select("fare_amount", "prediction", "features").write.parquet("results/predictions")

# Stop Spark session
spark.stop()
