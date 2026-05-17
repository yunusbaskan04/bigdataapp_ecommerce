from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Görevin genel kuralları
default_args = {
    'owner': 'yunus',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# DAG Tanımı
with DAG(
    'ecommerce_batch_analytics',
    default_args=default_args,
    description='Automates Spark Batch Job for E-Commerce Reports',
    schedule=None,  # Test aşamasında olduğumuz için otomatik çalışmasın, biz arayüzden butona basıp tetikleyeceğiz
    catchup=False,
    tags=['ecommerce', 'spark', 'postgres']
) as dag:

    # Task 1: Spark Batch Job'ı Tetikleyen İşçi
    # NOT: Otomasyon kodlarında docker komutunun sonundaki "-it" (interaktif terminal) ibaresini kaldırırız.
    run_spark_batch = BashOperator(
        task_id='run_spark_batch_job',
        bash_command=(
            'docker exec ecommerce-spark-app spark-submit '
            '--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.1 '
            '/app/batch_analyzer.py'
        )
    )

    # İş akış sırası (Şimdilik tek bir ana görevimiz var)
    run_spark_batch
