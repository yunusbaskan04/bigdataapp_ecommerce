# Temel olarak hafif bir Jupyter/PySpark imajı kullanıyoruz
FROM jupyter/pyspark-notebook:spark-3.5.0

# Gerekli Python kütüphanelerini kuruyoruz
# (MongoDB'ye ve Kafka'ya bağlanmak için)
USER root
RUN pip install kafka-python pymongo pyspark==3.5.0

# Kodlarımızı tutacağımız çalışma dizini
WORKDIR /app
USER jovyan
