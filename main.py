import requests
import pymongo
import os
import sys
from datetime import datetime
import pytz

# --- CẤU HÌNH BẢO MẬT ---
TOMTOM_KEY = os.getenv("TOMTOM_KEY")
MONGO_URI = os.getenv("MONGO_URI")

# Kiểm tra an toàn trước khi chạy
if not MONGO_URI or not TOMTOM_KEY:
    print("❌ LỖI: Thiếu biến môi trường (Secrets). Vui lòng kiểm tra GitHub Settings!")
    sys.exit(1)

def data_filter(raw_data):
    """Lọc và làm sạch dữ liệu với đầy đủ các trường thông tin"""
    confidence = raw_data.get('confidence', 0)
    if confidence < 0.1:
        return None 

    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz)
    
    v_curr = raw_data.get('currentSpeed', 0)
    v_free = raw_data.get('freeFlowSpeed', 0)
    
    # Tính toán chỉ số kẹt xe (Congestion Index)
    congestion_index = max(0, min(1, (v_free - v_curr) / v_free)) if v_free > 0 else 0
    
    return {
        "timestamp": now,
        "hour": now.hour,
        "day_of_week": now.weekday(),
        "is_weekend": now.weekday() >= 5,
        "current_speed": v_curr,
        "free_flow_speed": v_free,
        "congestion_index": round(congestion_index, 4),
        "confidence": confidence,
        # Thời gian di chuyển ước tính (phút/km)
        "travel_time_per_km": round(60 / v_curr, 2) if v_curr > 0 else 999.0 
    }

def run_harvest():
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client["SMSLOGS_DB01"]
        history_col = db["Traffic_History"]

        GO_VAP_SITES = {
            "Ngã_Sáu_Gò_Vấp": "10.8222,106.6775",
            "Quang_Trung_Thống_Nhất": "10.8265,106.6700",
            "Phan_Văn_Trị_Nguyễn_Oanh": "10.8272,106.6782",
            "Phạm_Văn_Đồng_Phan_Văn_Trị": "10.8200,106.6900"
        }

        print(f"🚀 Bắt đầu cào dữ liệu: {datetime.now()}")
        for site_name, coords in GO_VAP_SITES.items():
            url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key={TOMTOM_KEY}&point={coords}"
            res = requests.get(url, timeout=15)
            
            if res.status_code == 200:
                raw_traffic = res.json().get('flowSegmentData', {})
                processed_record = data_filter(raw_traffic)
                
                if processed_record:
                    processed_record["site"] = site_name
                    history_col.insert_one(processed_record)
                    print(f"✅ Đã lưu thành công: {site_name}")
            else:
                print(f"❌ Lỗi API tại {site_name}: {res.status_code}")
                
    except Exception as e:
        print(f"❌ Lỗi hệ thống: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_harvest()