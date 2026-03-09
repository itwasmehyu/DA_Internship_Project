import pandas as pd
import os

# Danh sách file CSV đã làm sạch
file_paths = {
    "AD_ACTION": "Dataset/cleaned/AD_ACTION.csv",
    "AD_REVENUE": "Dataset/cleaned/AD_REVENUE.csv",
    "COIN_GEM": "Dataset/cleaned/COIN_GEM.csv",
    "DAU": "Dataset/cleaned/DAU.csv",
    "ENGAGEMENT": "Dataset/cleaned/ENGAGEMENT.csv",
    "FIRST_OPEN": "Dataset/cleaned/FIRST_OPEN.csv",
    "REVENUE": "Dataset/cleaned/REVENUE.csv",
    "START_WIN_LOSE": "Dataset/cleaned/START_WIN_LOSE.csv",
    "TUTORIAL": "Dataset/cleaned/TUTORIAL.csv",
    "UNINSTALL_USER": "Dataset/cleaned/UNINSTALL_USER.csv"
}

# Danh sách file CSV gốc để so sánh
original_paths = {
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

for name in file_paths.keys():
    try:
        # Đọc file đã làm sạch
        cleaned_path = file_paths[name]
        original_path = original_paths[name]
        
        df_cleaned = pd.read_csv(cleaned_path, low_memory=False)
        df_original = pd.read_csv(original_path, low_memory=False)
        
        # Thống kê file đã làm sạch
        total_cells = df_cleaned.shape[0] * df_cleaned.shape[1]
        null_count = df_cleaned.isnull().sum().sum()
        null_pct = round((null_count / total_cells) * 100, 2) if total_cells > 0 else 0
        
        duplicate_count = df_cleaned.duplicated().sum()
        duplicate_pct = round((duplicate_count / len(df_cleaned)) * 100, 2) if len(df_cleaned) > 0 else 0
        
        # Xác định cột thời gian
        date_cols = []
        for col in df_cleaned.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ["date", "time", "created", "updated", "start", "end"]):
                date_cols.append(col)
        
        min_date, max_date = None, None
        for col in date_cols:
            try:
                df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors="coerce")
                valid_dates = df_cleaned[col].dropna()
                if len(valid_dates) > 0:
                    col_min = valid_dates.min()
                    col_max = valid_dates.max()
                    if min_date is None or col_min < min_date:
                        min_date = col_min
                    if max_date is None or col_max > max_date:
                        max_date = col_max
            except:
                pass
        
        # Kích thước file
        cleaned_size = os.path.getsize(cleaned_path)
        original_size = os.path.getsize(original_path)
        
        if cleaned_size < 1024**2:
            cleaned_size_str = f"{cleaned_size/1024:.2f} KB"
        else:
            cleaned_size_str = f"{cleaned_size/(1024**2):.2f} MB"
            
        if original_size < 1024**2:
            original_size_str = f"{original_size/1024:.2f} KB"
        else:
            original_size_str = f"{original_size/(1024**2):.2f} MB"
        
        # Tính % thay đổi
        rows_change = round(((len(df_cleaned) - len(df_original)) / len(df_original)) * 100, 2)
        size_change = round(((cleaned_size - original_size) / original_size) * 100, 2)
        
        summary.append({
            "File": name,
            "Dòng gốc": len(df_original),
            "Dòng sau làm sạch": len(df_cleaned),
            "% Thay đổi dòng": rows_change,
            "Số cột": df_cleaned.shape[1],
            "% Null": null_pct,
            "% Duplicate": duplicate_pct,
            "Ngày nhỏ nhất": min_date.strftime("%Y-%m-%d") if min_date else "N/A",
            "Ngày lớn nhất": max_date.strftime("%Y-%m-%d") if max_date else "N/A",
            "Kích thước gốc": original_size_str,
            "Kích thước sau làm sạch": cleaned_size_str,
            "% Thay đổi kích thước": size_change
        })
        
        print(f"✅ Đã xử lý: {name}")
        
    except Exception as e:
        print(f"❌ Lỗi khi xử lý {name}: {str(e)}")
        summary.append({
            "File": name,
            "Dòng gốc": "Lỗi",
            "Dòng sau làm sạch": "Lỗi",
            "% Thay đổi dòng": "Lỗi",
            "Số cột": "Lỗi",
            "% Null": "Lỗi",
            "% Duplicate": "Lỗi",
            "Ngày nhỏ nhất": "Lỗi",
            "Ngày lớn nhất": "Lỗi",
            "Kích thước gốc": "Lỗi",
            "Kích thước sau làm sạch": "Lỗi",
            "% Thay đổi kích thước": "Lỗi"
        })

# Tạo DataFrame và hiển thị
summary_df = pd.DataFrame(summary)

# Hiển thị đầy đủ tất cả cột
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

print("\n" + "="*100)
print("📊 THỐNG KÊ DỮ LIỆU SAU KHI LÀM SẠCH - SO SÁNH VỚI DỮ LIỆU GỐC")
print("="*100)
print(summary_df.to_string(index=False))

# Lưu ra file CSV
summary_df.to_csv("thong_ke_cleaned.csv", index=False, encoding='utf-8-sig')
print(f"\n�� Đã lưu kết quả vào file: thong_ke_cleaned.csv")

# Thống kê tổng quan
print("\n" + "="*80)
print("�� TỔNG KẾT QUÁ TRÌNH LÀM SẠCH DỮ LIỆU")
print("="*80)

total_original_rows = sum([row["Dòng gốc"] for row in summary if isinstance(row["Dòng gốc"], int)])
total_cleaned_rows = sum([row["Dòng sau làm sạch"] for row in summary if isinstance(row["Dòng sau làm sạch"], int)])

print(f"�� Tổng số dòng dữ liệu gốc: {total_original_rows:,}")
print(f"📊 Tổng số dòng sau làm sạch: {total_cleaned_rows:,}")
print(f"📊 Tổng số dòng bị mất: {total_original_rows - total_cleaned_rows:,}")
print(f"📊 Tỷ lệ giữ lại: {round((total_cleaned_rows/total_original_rows)*100, 2)}%")

# Kiểm tra chất lượng
print(f"\n�� CHẤT LƯỢNG DỮ LIỆU SAU LÀM SẠCH:")
print(f"📝 Số file có % null = 0: {len([row for row in summary if row['% Null'] == 0])}")
print(f"📝 Số file có % duplicate = 0: {len([row for row in summary if row['% Duplicate'] == 0])}")
print(f"📝 Số file có % null < 1: {len([row for row in summary if isinstance(row['% Null'], (int, float)) and row['% Null'] < 1])}")