from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

# Spark Session - Fabrikayı ayağa kaldırıyoruz
spark = SparkSession.builder \
    .appName("EcommerceStreamProcessor") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://ecommerce-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "secretpassword") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

# Kafka'dan Veri Okuma (Musluk)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "ecommerce-kafka-broker:9092") \
    .option("subscribe", "clickstream-data") \
    .load()

# Şema Tanımlama (Anlamlandırma)
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("session_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("action", StringType()) \
    .add("item", StringType()) \
    .add("category", StringType()) \
    .add("device_type", StringType()) \
    .add("location", StringType())

# Veriyi Dönüştürme
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# MongoDB'ye Yazma (Baraj)
query = parsed_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/pyspark_checkpoints") \
    .option("connection.uri", "mongodb://admin:secretpassword@ecommerce-mongodb:27017") \
    .option("database", "ecommerce_db") \
    .option("collection", "ecommerce_logs") \
    .option("authSource", "admin") \
    .outputMode("append") \
    .start()

# MinIO (Data Lake) Arşivleme - PARQUET FORMATI
query_minio = parsed_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/pyspark_checkpoints_minio") \
    .option("path", "s3a://clickstream-raw/events") \
    .start()

# İki sorguyu da bekletmek için
spark.streams.awaitAnyTermination()

