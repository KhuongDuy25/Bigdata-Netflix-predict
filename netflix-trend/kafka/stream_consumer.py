import json
from kafka import KafkaConsumer
from pymongo import MongoClient

# Kết nối MongoDB
mongo_client = MongoClient("mongodb://mongo:27017/")
db = mongo_client["netflix_trend"]  #tao database netflix_trend
collection = db["search_logs"] #tao collection search_logs

# Kết nối Kafka
consumer = KafkaConsumer(
    "netflix_search", #ket noi toi topic netflix_search
    bootstrap_servers=["kafka:9092"], #dia chi cua kafka
    value_deserializer=lambda m: json.loads(m.decode("utf-8")), #chuyen doi gia tri nhan duoc tu json sang dict
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="netflix_group"
)

print("🟢 Consumer đang chạy và chờ dữ liệu...")

for message in consumer:
    data = message.value
    collection.insert_one(data)
    print(f"Đã lưu vào MongoDB: {data}")