import requests
import pymongo
import os
from datetime import datetime
import pytz

# LẤY BIẾN TỪ GITHUB SECRETS (Bảo mật)
TOMTOM_KEY = os.getenv("TOMTOM_KEY")
MONGO_URI = os.getenv("MONGO_URI")

def run_scraper():
    # 1. Kết nối DB
    client = pymongo.MongoClient(MONGO_URI)
    db = client["SMSLOGS_DB01"]
    history_col = db["Traffic_History"]

    # 2. Danh sách tọa độ Gò Vấp
    GO_VAP_SITES = {
        "Ngã_Sáu_Gò_Vấp": "10.8222,106.6775",
        "Quang_Trung_Thống_Nhất": "10.8265,106.6700",
        "Phan_Văn_Trị_Nguyễn_Oanh": "10.8272,106.6782",
        "Phạm_Văn_Đồng_Phan_Văn_Trị": "10.8200,106.6900"
    }

    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz)

    for site_name, coords in GO_VAP_SITES.items():
        url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key={TOMTOM_KEY}&point={coords}"
        try:
            res = requests.get(url, timeout=15)
            if res.status_code == 200:
                data = res.json().get('flowSegmentData', {})
                v_curr = data.get('currentSpeed', 0)
                v_free = data.get('freeFlowSpeed', 0)
                
                # Tính chỉ số kẹt xe $CI$
                ci = max(0, min(1, (v_free - v_curr) / v_free)) if v_free > 0 else 0
                
                record = {
                    "site": site_name,
                    "timestamp": now,
                    "hour": now.hour,
                    "day_of_week": now.weekday(),
                    "congestion_index": round(ci, 4),
                    "current_speed": v_curr
                }
                history_col.insert_one(record)
                print(f"✅ Đã lưu: {site_name}")
        except Exception as e:
            print(f"❌ Lỗi tại {site_name}: {e}")

if __name__ == "__main__":
    run_scraper()