import time
import json
import random
import requests
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime, timedelta, timezone

fake = Faker()

# === LẤY DỮ LIỆU TỪ API TMDb === #
API_KEY = "133eaafc7d07ebd6090bad2396db5271"
# MOVIE_URL = f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}&language=en-US&page=1"
# GENRE_URL = f"https://api.themoviedb.org/3/genre/movie/list?api_key={API_KEY}&language=en-US"

# Lấy danh sách thể loại
#genre_resp = requests.get(GENRE_URL)
# genre_dict = {g['id']: g['name'] for g in genre_resp.json().get('genres', [])}
#genres = [g['name'] for g in genre_resp.json().get('genres', [])]

# Nếu muốn chỉ lấy các thể loại phổ biến nhất:
top_genres = {"Action", "Drama", "Horror",}
#genres = [g for g in genres if g in top_genres]


# movie_resp = requests.get(MOVIE_URL)
# movies = []
# for page in [3, 4, 5]:
#     movie_resp = requests.get(MOVIE_URL.replace("page=1", f"page={page}"))
#     for m in movie_resp.json().get('results', []):
#         genres = [genre_dict.get(gid, "Unknown") for gid in m.get('genre_ids', [])]
#         movies.append({
#             "original_title": m['original_title'],
#             "genre": genres[0] if genres else "Unknown",
#             "release_date": m.get('release_date', None)
#         })

# Giả lập dữ liệu
regions = ["US", "VN", "KR"]
devices = ["mobile", "web", "tv"]

# Thông tin Kafka
KAFKA_TOPIC = "netflix_search" # Tên topic Kafka để gửi dữ liệu
KAFKA_BROKER = "kafka:9092" # Địa chỉ Kafka broker

# Khởi tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# === GỬI DỮ LIỆU GIẢ LẬP LỊCH SỬ (dữ liệu quá khứ) === #
def send_history():
    print("📦 Gửi dữ liệu lịch sử giả lập...")
    for _ in range(2000):  # Gửi 2000 bản ghi lịch sử, chỉ chạy 1 lần
        days_ago = random.randint(0, 6)
        random_time = datetime.now(timezone.utc) - timedelta(
            days=days_ago,
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        data = {
            "user_id": fake.uuid4(),
            "genre": random.choice(list(top_genres)),
            "region": random.choice(regions),
            "device": random.choice(devices),
            "search_time": random_time.isoformat()
        }
        producer.send(KAFKA_TOPIC, value=data)
        print(f" [Lịch sử] Sent: {data}")

def send_realtime():
    print(f"🔁 Bắt đầu gửi dữ liệu realtime vào Kafka topic: {KAFKA_TOPIC}")
    while True:
        for _ in range(10):
            data = {
                "user_id": fake.uuid4(),
                "genre": random.choice(list(top_genres)),
                "region": random.choice(regions),
                "device": random.choice(devices),
                "search_time": datetime.now(timezone.utc).isoformat()
            }
            producer.send(KAFKA_TOPIC, value=data)
            print(f" [Realtime] Sent: {data}")
        time.sleep(1)
if __name__ == "__main__":
    send_history()      # Gửi lịch sử 1 lần
    time.sleep(2)       # Đợi 1 giây
    send_realtime()     # Sau đó gửi realtime liên tục