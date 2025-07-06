from pymongo import MongoClient
import pandas as pd
import time

# Kết nối MongoDB
mongo_client = MongoClient("mongodb://mongo:27017/")
db = mongo_client["netflix_trend"]
raw_collection = db["search_logs"]
clean_collection = db["clean_search_logs"]

while True:
    # Lấy dữ liệu từ collection gốc
    data = list(raw_collection.find())
 

    # Xử lý dữ liệu: loại bỏ bản ghi thiếu trường quan trọng
    df = df.dropna(subset=["user_id", "genre", "region", "device", "search_time"])

    # (Có thể) Chuẩn hóa tên phim, thể loại, v.v.
    # df["original_title"] = df["original_title"].str.strip().str.title()
    df["genre"] = df["genre"].str.strip().str.title()

    # Xóa các trường MongoDB mặc định nếu không cần
    if "_id" in df.columns:
        df = df.drop(columns=["_id"])

    # Lưu dữ liệu sạch vào collection mới
    clean_collection.delete_many({})
    if not df.empty:
        clean_collection.insert_many(df.to_dict("records"))

    print(" Đã ETL và lưu dữ liệu sạch vào MongoDB!")
    time.sleep(3)