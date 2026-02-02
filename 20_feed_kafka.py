from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col

# Define the schema to match the emulator output
sensor_schema = StructType([
    StructField("SensorID", StringType(), False),
    StructField("StartTime", TimestampType(), True),
    StructField("EndTime", TimestampType(), True),
    StructField("Status", StringType(), False),
    StructField("Severity", StringType(), True)
])

spark = SparkSession.builder \
    .appName("SensorProcessing") \
    .config("spark.sql.streaming.checkpointLocation", "./checkpoints") \
    .getOrCreate()

# Read from the directory where the emulator drops files
raw_stream = spark.readStream \
    .schema(sensor_schema) \
    .parquet("./data/")

# Write to Kafka
query = raw_stream.selectExpr("CAST(SensorID AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "sensor-updates") \
    .start()

query.awaitTermination()
