from pymongo import MongoClient
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report

# Kết nối MongoDB
mongo_client = MongoClient("mongodb://mongo:27017/")
db = mongo_client["netflix_trend"]
collection = db["clean_search_logs"]

# Lấy dữ liệu sạch
data = list(collection.find())
df = pd.DataFrame(data)

# Phân tích nhanh: Thể loại nào được tìm kiếm nhiều nhất
print("Top thể loại được tìm kiếm nhiều nhất:")
print(df["genre"].value_counts().head())

# Tiền xử lý dữ liệu thời gian
df['search_time'] = pd.to_datetime(df['search_time'], utc=True, errors='coerce')
df['day_of_week'] = df['search_time'].dt.dayofweek  # 0 = Monday, 6 = Sunday
df['is_weekend'] = df['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)

# Encode các trường dạng text thành số
le_region = LabelEncoder()
le_device = LabelEncoder()
le_genre = LabelEncoder()

df["region_enc"] = le_region.fit_transform(df["region"])
df["device_enc"] = le_device.fit_transform(df["device"])
df["genre_enc"] = le_genre.fit_transform(df["genre"])

# Loại bỏ bản ghi thiếu dữ liệu
df = df.dropna(subset=["region", "device", "genre", "day_of_week"])

# Tạo tập đặc trưng và nhãn (chỉ theo ngày)
X = df[["region_enc", "device_enc", "day_of_week", "is_weekend"]]
y = df["genre_enc"]

# Chia dữ liệu train/test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Huấn luyện mô hình Logistic Regression
model = LogisticRegression(max_iter=1000, random_state=42, class_weight='balanced')
model.fit(X_train, y_train)

# Đánh giá mô hình
y_pred = model.predict(X_test)
print(classification_report(y_test, y_pred, target_names=le_genre.classes_))

# Dự đoán 
region = "VN"
device = "web"
search_time = pd.to_datetime("2025-06-25")
day_of_week = search_time.dayofweek  # Kết quả: 4 (ngày số 4 trong tuần tức thứ 6)
is_weekend = 1 if day_of_week >= 5 else 0

sample = pd.DataFrame([{
    "region_enc": le_region.transform([region])[0],
    "device_enc": le_device.transform([device])[0],
    "day_of_week": day_of_week,
    "is_weekend": is_weekend
}])

pred_genre_enc = model.predict(sample)[0]
pred_genre = le_genre.inverse_transform([pred_genre_enc])[0]

print(f"Dự đoán thể loại cho quốc gia {region}, thiết bị {device}, thứ {day_of_week+2} {'(cuối tuần)' if is_weekend else ''}: {pred_genre}")