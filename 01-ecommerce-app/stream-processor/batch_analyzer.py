from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, hour, to_timestamp, sum as _sum

# SparkSession Yapılandırması (MinIO ve Postgres Destekli)
spark = SparkSession.builder \
    .appName("EcommerceRealWorldBatch") \
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

# 🚀 ANALİZ 1: Dönüşüm Hunisi (Conversion Funnel)
# Müşteriler ürünleri sadece görüntülüyor mu, sepete mi atıyor, yoksa alıyor mu?
funnel_df = raw_df.groupBy("event_type") \
    .agg(count("*").alias("total_events")) \
    .orderBy(col("total_events").desc())

# 🚀 ANALİZ 2: Marka Rekabeti ve Ciro Dağılımı (Top Brands by Revenue)
# Sadece satışı gerçekleşmiş (purchase) ürünleri filtrele, markalara göre grupla ve toplam ciroya bak
revenue_df = raw_df.filter(col("event_type") == "purchase") \
    .filter(col("brand") != "no-brand") \
    .groupBy("brand") \
    .agg(
        _sum("price").alias("total_revenue"),
        count("*").alias("total_sales")
    ) \
    .orderBy(col("total_revenue").desc()) \
    .limit(10) # Sadece en iyi 10 markayı al

print("📊 Analiz tamamlandı. Sonuçlar PostgreSQL'e yazılıyor...")

# 2. Sonuçları PostgreSQL'e Yaz (Data Warehouse)

# Huniyi Yaz
funnel_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://ecommerce-postgres:5432/ecommerce_reports") \
    .option("dbtable", "funnel_analysis") \
    .option("user", "admin") \
    .option("password", "secretpassword") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

# Ciro Raporunu Yaz
revenue_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://ecommerce-postgres:5432/ecommerce_reports") \
    .option("dbtable", "brand_revenue") \
    .option("user", "admin") \
    .option("password", "secretpassword") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("✅ Raporlar hazır! Fabrika gece mesaisini başarıyla tamamladı.")
