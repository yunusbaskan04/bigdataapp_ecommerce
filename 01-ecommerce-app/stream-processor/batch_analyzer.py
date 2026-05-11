from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, hour, to_timestamp

# SparkSession Yapılandırması (MinIO ve Postgres Destekli)
spark = SparkSession.builder \
    .appName("EcommerceBatchAnalyzer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://ecommerce-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "secretpassword") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

print("📂 MinIO'dan ham veriler okunuyor...")

# 1. MinIO'dan Parquet Dosyalarını Oku
raw_df = spark.read.parquet("s3a://clickstream-raw/events")

# 2. Ağır Analiz: Şehir Bazlı En Çok Aranan Kategoriler
city_trends = raw_df.groupBy("location", "category").agg(count("*").alias("total_count")) \
    .orderBy(col("total_count").desc())

# 3. Ağır Analiz: Günün Hangi Saatleri En Yoğun?
time_analysis = raw_df.withColumn("event_time", col("timestamp").cast("timestamp")) \
    .withColumn("event_hour", hour("event_time")) \
    .groupBy("event_hour").agg(count("*").alias("hourly_traffic")) \
    .orderBy("event_hour")

print("📊 Analiz tamamlandı. Sonuçlar PostgreSQL'e yazılıyor...")

# 4. Sonuçları PostgreSQL'e Yaz (Data Warehouse)
city_trends.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://ecommerce-postgres:5432/ecommerce_reports") \
    .option("dbtable", "city_category_trends") \
    .option("user", "admin") \
    .option("password", "secretpassword") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

# Rapor 2: Saatlik Yoğunluk Analizi (YENİ EKLENEN KISIM)
time_analysis.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://ecommerce-postgres:5432/ecommerce_reports") \
    .option("dbtable", "time_analysis") \
    .option("user", "admin") \
    .option("password", "secretpassword") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("✅ Raporlar hazır! Fabrika gece mesaisini başarıyla tamamladı.")
