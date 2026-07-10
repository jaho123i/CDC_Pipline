from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder \
    .appName("DebeziumStreaming") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

after_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("created_at", StringType(), True)
])

payload_schema = StructType([
    StructField("op", StringType(), True),
    StructField("after", after_schema, True)
])

debezium_schema = StructType([
    StructField("payload", payload_schema, True)
])

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "pg.demo.customers") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df_raw \
    .selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json(col("json_str"), debezium_schema).alias("data")) \
    .select("data.payload.op", "data.payload.after.*") \
    .filter(col("op").isNotNull())

query = df_parsed.writeStream \
    .format("parquet") \
    .option("path", "s3a://datalake/customers/raw/") \
    .option("checkpointLocation", "s3a://datalake/checkpoints/customers/") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()