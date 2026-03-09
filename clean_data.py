import pandas as pd
import os
from datetime import datetime

# Thư mục chứa CSV gốc
folder_path = "Dataset"
# Thư mục lưu CSV đã làm sạch
clean_folder = os.path.join(folder_path, "cleaned")
os.makedirs(clean_folder, exist_ok=True)

# Danh sách file
files = [
    "AD_ACTION.csv",
    "AD_REVENUE.csv",
    "COIN_GEM.csv",
    "DAU.csv",
    "ENGAGEMENT.csv",
    "FIRST_OPEN.csv",
    "REVENUE.csv",
    "START_WIN_LOSE.csv",
    "TUTORIAL.csv",
    "UNINSTALL_USER.csv"
]

# Hàm phát hiện cột thời gian
def detect_datetime_columns(df):
    datetime_cols = []
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            datetime_cols.append(col)
    return datetime_cols

# Làm sạch dữ liệu
cleaned_data = {}

for file in files:
    try:
        path = os.path.join(folder_path, file)
        print(f"🔄 Đang xử lý: {file}")
        
        # Đọc file
        df = pd.read_csv(path, low_memory=False)
        original_rows = len(df)
        
        # Chuẩn hóa user_id (nếu có)
        user_id_cols = [col for col in df.columns if "user_id" in col.lower()]
        for col in user_id_cols:
            df[col] = df[col].astype(str).str.strip()

        # Xử lý cột thời gian (chỉ chuyển đổi, không lọc)
        datetime_cols = detect_datetime_columns(df)
        for col in datetime_cols:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                print(f"   📅 Chuyển đổi cột thời gian: {col}")
            except Exception as e:
                print(f"   ⚠️ Không thể chuyển đổi cột {col}: {e}")

        # Xử lý null - chỉ loại bỏ nếu user_id null (nếu có)
        if user_id_cols:
            df = df.dropna(subset=user_id_cols)
            print(f"   🧹 Loại bỏ dòng có user_id null")
        
        # Fill null cho số
        num_cols = df.select_dtypes(include=['float64', 'int64']).columns
        if len(num_cols) > 0:
            df[num_cols] = df[num_cols].fillna(0)
            print(f"    Fill null cho {len(num_cols)} cột số")

        # Fill null cho text
        obj_cols = df.select_dtypes(include=['object']).columns
        if len(obj_cols) > 0:
            df[obj_cols] = df[obj_cols].fillna("unknown")
            print(f"    Fill null cho {len(num_cols)} cột text")

        # Xử lý duplicate
        before_duplicates = len(df)
        df = df.drop_duplicates()
        after_duplicates = before_duplicates - len(df)
        if after_duplicates > 0:
            print(f"   ️ Loại bỏ {after_duplicates} dòng trùng lặp")

        # Lưu vào dict
        cleaned_data[file.replace(".csv", "")] = df

        # Lưu file sạch vào thư mục cleaned
        save_path = os.path.join(clean_folder, file)
        df.to_csv(save_path, index=False, encoding="utf-8-sig")
        
        final_rows = len(df)
        print(f"✅ Đã lưu file sạch: {save_path}")
        print(f"   📊 Dòng gốc: {original_rows} → Dòng sau làm sạch: {final_rows}")
        print(f"   💾 Kích thước: {os.path.getsize(save_path) / (1024*1024):.2f} MB")
        print("-" * 50)
        
    except Exception as e:
        print(f"❌ Lỗi khi xử lý {file}: {str(e)}")
        print("-" * 50)

print("🎯 Hoàn tất làm sạch và lưu toàn bộ file.")
print(f"📁 Dữ liệu đã làm sạch được lưu tại: {clean_folder}")

# Thống kê tổng quan
print("\n THỐNG KÊ SAU KHI LÀM SẠCH:")
print("=" * 60)
for name, df in cleaned_data.items():
    print(f"{name}: {len(df)} dòng, {df.shape[1]} cột")
