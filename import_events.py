from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, to_json

# Spark 4.1.1 Batch Session
spark = SparkSession.builder \
    .appName("SensorBatchProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .config("spark.sql.jsonGenerator.ignoreNullFields", "false") \
    .config("spark.executor.memory", "32g") \
    .config("spark.driver.memory", "32g") \
    .config("spark.executor.memoryOverhead", "8g") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "8g") \
    .getOrCreate()

# 1. Read all parquet files in the directory as a single DataFrame
df = spark.read.parquet("./data/endpoint_events/")

# 2. Sort globally by UpdatedAt
sorted_df = df.orderBy(col("UpdatedAt").asc())

# 3. Prepare for Kafka: Key must be String/Binary, Value must be String/Binary
kafka_df = sorted_df.selectExpr(
    "CAST(EventID AS STRING) AS key", 
    "to_json(struct(*)) AS value"
)

# 4. Write to Kafka and close
# kafka_df.show(5, truncate=False)
kafka_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "events") \
    .option("kafka.enable.idempotence", "false") \
    .save()

print("Data successfully sorted and pushed to Kafka.")
spark.stop()