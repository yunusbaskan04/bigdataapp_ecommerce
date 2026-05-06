import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker('tr_TR')

producer = KafkaProducer(
    bootstrap_servers=['ecommerce-kafka-broker:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

ACTIONS = ['search', 'view_item', 'add_to_cart', 'checkout']
DEVICES = ['Mobile', 'Web', 'Tablet']
CATEGORIES = {
    'Elektronik': ['Laptop', 'Akıllı Telefon', 'Kulaklık', 'Monitör'],
    'Giyim': ['Tişört', 'Kot Pantolon', 'Ceket', 'Spor Ayakkabı'],
    'Ev_Yasam': ['Çalışma Masası', 'Oyuncu Koltuğu', 'Masa Lambası', 'Kitaplık']
}

# YENİ: Gerçekçi Bölgesel Analiz İçin Sabit Şehir Listesi
CITIES = ['İstanbul', 'Ankara', 'İzmir', 'Bursa', 'Antalya', 'Adana', 'Kayseri', 'Gaziantep', 'Konya', 'Trabzon']

print("🔥 E-Ticaret Simülasyonu Başlıyor... (Durdurmak için Ctrl+C)")

try:
    while True:
        user_id = random.randint(1, 5000)
        action = random.choice(ACTIONS)
        device = random.choice(DEVICES)
        category = random.choice(list(CATEGORIES.keys()))
        item = random.choice(CATEGORIES[category])
        city = random.choice(CITIES) # YENİ: Listeden rastgele gerçek şehir seçimi
        
        payload = {
            "user_id": user_id,
            "session_id": str(fake.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "action": action,
            "item": item,
            "category": category,
            "device_type": device,
            "location": city # YENİ: Artık Lake Yağıntown yok :)
        }
        
        producer.send('clickstream-data', value=payload)
        print(f"🚀 Gönderildi: {payload}")
        
        time.sleep(random.uniform(0.1, 0.5))
        
except KeyboardInterrupt:
    print("\n🛑 Simülasyon durduruldu.")
finally:
    producer.close()
