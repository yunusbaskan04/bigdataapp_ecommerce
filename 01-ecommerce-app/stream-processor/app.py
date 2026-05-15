from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, when
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# 1. Spark Session
spark = SparkSession.builder \
    .appName("EcommerceRealWorldStreaming") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://ecommerce-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "secretpassword") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Kafka'dan Veri Okuma
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "ecommerce-kafka-broker:9092") \
    .option("subscribe", "clickstream-data") \
    .load()

# 3. Kaggle V2 Şeması
schema = StructType() \
    .add("event_time", StringType()) \
    .add("event_type", StringType()) \
    .add("product_id", IntegerType()) \
    .add("category_id", StringType()) \
    .add("category_code", StringType()) \
    .add("brand", StringType()) \
    .add("price", DoubleType()) \
    .add("user_id", IntegerType()) \
    .add("user_session", StringType())

# 4. Veri Dönüştürme
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss 'UTC'")) \
    .fillna({"category_code": "unknown", "brand": "no-brand"})
# 5. MongoDB Sink
query_mongo = parsed_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/pyspark_checkpoints_mongo") \
    .option("connection.uri", "mongodb://admin:secretpassword@ecommerce-mongodb:27017") \
    .option("database", "ecommerce_db") \
    .option("collection", "ecommerce_logs") \
    .option("authSource", "admin") \
    .outputMode("append") \
    .start()

# 6. MinIO Sink
query_minio = parsed_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/pyspark_checkpoints_minio") \
    .option("path", "s3a://clickstream-raw/events") \
    .start()

print("🚀 V2 Boru Hattı Ateşlendi! Veriler akıyor...")

spark.streams.awaitAnyTermination()
