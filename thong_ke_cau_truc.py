import pandas as pd
import os

# Danh sách file CSV và đường dẫn (thay bằng đường dẫn file của bạn)
file_paths = {
    "AD_ACTION": "Dataset/AD_ACTION.csv",
    "AD_REVENUE": "Dataset/AD_REVENUE.csv",
    "COIN_GEM": "Dataset/COIN_GEM.csv",
    "DAU": "Dataset/DAU.csv",
    "ENGAGEMENT": "Dataset/ENGAGEMENT.csv",
    "FIRST_OPEN": "Dataset/FIRST_OPEN.csv",
    "REVENUE": "Dataset/REVENUE.csv",
    "START_WIN_LOSE": "Dataset/START_WIN_LOSE.csv",
    "TUTORIAL": "Dataset/TUTORIAL.csv",
    "UNINSTALL_USER": "Dataset/UNINSTALL_USER.csv"
}

summary = []

for name, path in file_paths.items():
    try:
        # Đọc file
        df = pd.read_csv(path, low_memory=False)
        
        # Tính số null (%)
        total_cells = df.shape[0] * df.shape[1]
        null_count = df.isnull().sum().sum()
        null_pct = round((null_count / total_cells) * 100, 2) if total_cells > 0 else 0
        
        # Tính % duplicate
        duplicate_count = df.duplicated().sum()
        duplicate_pct = round((duplicate_count / len(df)) * 100, 2) if len(df) > 0 else 0
        
        # Xác định cột thời gian (tự động dò dựa vào tên hoặc dtype)
        date_cols = []
        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ["date", "time", "created", "updated", "start", "end"]):
                date_cols.append(col)
        
        min_date, max_date = None, None
        for col in date_cols:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                valid_dates = df[col].dropna()
                if len(valid_dates) > 0:
                    col_min = valid_dates.min()
                    col_max = valid_dates.max()
                    if min_date is None or col_min < min_date:
                        min_date = col_min
                    if max_date is None or col_max > max_date:
                        max_date = col_max
            except:
                pass
        
        # Kích thước file KB/MB
        file_size = os.path.getsize(path)
        if file_size < 1024**2:
            size_str = f"{file_size/1024:.2f} KB"
        else:
            size_str = f"{file_size/(1024**2):.2f} MB"
        
        summary.append({
            "File": name,
            "Số dòng": len(df),
            "Số cột": df.shape[1],
            "% Null": null_pct,
            "% Duplicate": duplicate_pct,
            "Ngày nhỏ nhất": min_date.strftime("%Y-%m-%d") if min_date else "N/A",
            "Ngày lớn nhất": max_date.strftime("%Y-%m-%d") if max_date else "N/A",
            "Kích thước file": size_str
        })
        
        print(f"Đã xử lý: {name}")
        
    except Exception as e:
        print(f"Lỗi khi xử lý {name}: {str(e)}")
        summary.append({
            "File": name,
            "Số dòng": "Lỗi",
            "Số cột": "Lỗi",
            "% Null": "Lỗi",
            "% Duplicate": "Lỗi",
            "Ngày nhỏ nhất": "Lỗi",
            "Ngày lớn nhất": "Lỗi",
            "Kích thước file": "Lỗi"
        })

# Tạo DataFrame và hiển thị
summary_df = pd.DataFrame(summary)

# Hiển thị đầy đủ tất cả cột
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

print("\n" + "="*80)
print("THỐNG KÊ CẤU TRÚC DỮ LIỆU")
print("="*80)
print(summary_df.to_string(index=False))

# Lưu ra file CSV
summary_df.to_csv("thong_ke_cau_truc.csv", index=False, encoding='utf-8-sig')
print(f"\nĐã lưu kết quả vào file: thong_ke_cau_truc.csv")
