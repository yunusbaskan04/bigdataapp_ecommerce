import csv
import json
import time
from kafka import KafkaProducer

# Kafka Ayarları
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "ecommerce-events"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# İndireceğimiz verinin ilk 1 milyon satırlık test versiyonu
DATA_FILE = "sample_data.csv" 

print(f"🚀 Gerçek veri akışı başlatılıyor... Hedef: {TOPIC_NAME}")

def stream_real_data():
    try:
        # csv.DictReader, devasa dosyaları satır satır okur, RAM'i şişirmez.
        with open(DATA_FILE, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            
            for count, row in enumerate(csv_reader, 1):
                # Veriyi Kafka'ya bas
                producer.send(TOPIC_NAME, row)
                
                # Her 10.000 satırda bir ekrana bilgi bas (konsol donmasın diye)
                if count % 10000 == 0:
                    print(f"✅ {count} satır Kafka'ya gönderildi...")
                    
                # Gerçek zaman simülasyonu için çok ufak bir bekleme (Saniyede ~100 mesaj)
                time.sleep(0.01)

    except FileNotFoundError:
        print(f"❌ HATA: {DATA_FILE} dosyası bulunamadı! Veriyi aynı klasöre koyduğundan emin ol.")
    except Exception as e:
        print(f"❌ Beklenmeyen Hata: {e}")

if __name__ == "__main__":
    stream_real_data()
    # İşlem bitince Kafka bağlantısını güvenli kapat
    producer.flush()
    producer.close()
    print("🏁 Veri akışı tamamlandı!")
