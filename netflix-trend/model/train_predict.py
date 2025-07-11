from pymongo import MongoClient
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
import xgboost as xgb
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

# Tiền xử lý dữ liệu
df['search_time'] = pd.to_datetime(df['search_time'], utc=True, errors='coerce')
df['hour'] = df['search_time'].dt.hour
df['day_of_week'] = df['search_time'].dt.dayofweek  # 0 = Monday, 6 = Sunday
df['is_weekend'] = df['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)

# Gom nhóm giờ thành các khung (sáng, chiều, tối, đêm)
def hour_to_period(hour):
    if 5 <= hour < 12:
        return "morning"
    elif 12 <= hour < 18:
        return "afternoon"
    elif 18 <= hour < 23:
        return "evening"
    else:
        return "night"
df['hour_period'] = df['hour'].apply(hour_to_period)

# Encode các trường dạng text thành số
le_region = LabelEncoder()
le_device = LabelEncoder()
le_hour_period = LabelEncoder()
le_genre = LabelEncoder()

df['hour_period_enc'] = le_hour_period.fit_transform(df['hour_period'])
df["region_enc"] = le_region.fit_transform(df["region"])
df["device_enc"] = le_device.fit_transform(df["device"])
df["genre_enc"] = le_genre.fit_transform(df["genre"])

# Loại bỏ bản ghi thiếu trường quan trọng
df = df.dropna(subset=["region", "device", "hour", "genre", "day_of_week", "hour_period"])

# Tạo tập đặc trưng và nhãn (kết hợp ngày, giờ, vùng, thiết bị)
X = df[["region_enc", "device_enc", "hour_period_enc", "day_of_week", "is_weekend"]]
y = df["genre_enc"]

# Chia train/test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Huấn luyện mô hình
model = LogisticRegression(max_iter=1000, random_state=42, class_weight='balanced')
model.fit(X_train, y_train)

# model = xgb.XGBClassifier(
#     n_estimators=100,
#     max_depth=5,
#     learning_rate=0.1,
#     objective='multi:softmax',
#     num_class=len(le_genre.classes_),
#     random_state=42,
#     use_label_encoder=False,
#     eval_metric='mlogloss'
# )
# model.fit(X_train, y_train)

# Đánh giá mô hình
y_pred = model.predict(X_test)
print(classification_report(y_test, y_pred, target_names=le_genre.classes_))

# Ví dụ dự đoán cho một trường hợp mới
region = "VN"
device = "web"
hour = 3
hour_period = hour_to_period(hour)
search_time = pd.to_datetime("2025-06-29")
day_of_week = search_time.dayofweek # tra ve so ng tuan (0 = Monday, 6 = Sunday)
is_weekend = 1 if day_of_week >= 5 else 0

sample = pd.DataFrame([{
    "region_enc": le_region.transform([region])[0],
    "device_enc": le_device.transform([device])[0],
    "hour_period_enc": le_hour_period.transform([hour_period])[0],
    "day_of_week": day_of_week,
    "is_weekend": is_weekend
}])
pred_genre_enc = model.predict(sample)[0]
pred_genre = le_genre.inverse_transform([pred_genre_enc])[0] 


print(f"Dự đoán thể loại cho quốc gia {region}, thiết bị {device}, khung giờ '{hour_period}', thứ {day_of_week+2} {'(cuối tuần)' if is_weekend else ''}: {pred_genre}")