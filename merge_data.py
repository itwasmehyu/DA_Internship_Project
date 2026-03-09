import pandas as pd
import os
from datetime import datetime

# Thư mục chứa CSV đã làm sạch
cleaned_folder = "Dataset/cleaned"
output_folder = "Dataset/merged"
os.makedirs(output_folder, exist_ok=True)

print("🔄 Bắt đầu quá trình Merge/Join các bảng...")

# 1. Đọc bảng chính (FIRST_OPEN)
print("📖 Đang đọc bảng chính: FIRST_OPEN")
master_df = pd.read_csv(f"{cleaned_folder}/FIRST_OPEN.csv", low_memory=False)
print(f"   📊 Số dòng: {len(master_df):,}")

# 2. Đọc các bảng cần merge
tables = {
    "AD_ACTION": ["status", "location", "type", "SOURCE", "platform", "action"],
    "AD_REVENUE": ["value", "duration", "ad_platform", "ad_format"],
    "COIN_GEM": ["currency", "value", "total", "type", "position"],
    "DAU": [],  # Chỉ có thông tin cơ bản
    "ENGAGEMENT": ["engagement_time_sec"],
    "REVENUE": ["revenue", "product_id"],
    "START_WIN_LOSE": ["level", "win", "lose", "start", "win_time", "lose_time", "stream_id", "mode_play"],
    "TUTORIAL": ["tut_id", "time", "tut_status"],
    "UNINSTALL_USER": []  # Chỉ có thông tin cơ bản
}

# 3. Thực hiện merge từng bảng
merged_df = master_df.copy()
print(f"\n🔄 Bắt đầu merge các bảng...")

for table_name, specific_cols in tables.items():
    try:
        print(f"\n�� Đang xử lý: {table_name}")
        
        # Đọc bảng
        table_path = f"{cleaned_folder}/{table_name}.csv"
        if os.path.exists(table_path):
            table_df = pd.read_csv(table_path, low_memory=False)
            print(f"   📊 Số dòng: {len(table_df):,}")
            
            # Chọn cột cần thiết
            key_cols = ["user_pseudo_id", "event_date"]
            if "event_date" not in table_df.columns:
                key_cols = ["user_pseudo_id", "parse_event_date"]
            
            # Chọn cột để merge
            cols_to_merge = key_cols + specific_cols
            available_cols = [col for col in cols_to_merge if col in table_df.columns]
            
            if len(available_cols) > len(key_cols):
                merge_df = table_df[available_cols].copy()
                
                # Chuẩn hóa tên cột để tránh trùng
                for col in specific_cols:
                    if col in merge_df.columns:
                        new_col_name = f"{table_name.lower()}_{col}"
                        merge_df = merge_df.rename(columns={col: new_col_name})
                
                # Merge với bảng chính
                before_merge = len(merged_df)
                merged_df = pd.merge(
                    merged_df, 
                    merge_df, 
                    on=key_cols[0],  # Chỉ merge theo user_pseudo_id
                    how='left',
                    suffixes=('', f'_{table_name.lower()}')
                )
                after_merge = len(merged_df)
                
                print(f"   ✅ Merge thành công: {len(available_cols)-len(key_cols)} cột")
                print(f"   📊 Dòng trước: {before_merge:,} → Dòng sau: {after_merge:,}")
            else:
                print(f"   ⚠️ Không có cột đặc biệt để merge")
        else:
            print(f"   ❌ Không tìm thấy file: {table_path}")
            
    except Exception as e:
        print(f"   ❌ Lỗi khi xử lý {table_name}: {str(e)}")

# 4. Xử lý dữ liệu sau merge
print(f"\n�� Đang xử lý dữ liệu sau merge...")

# Loại bỏ cột trùng lặp
duplicate_cols = merged_df.columns[merged_df.columns.duplicated()]
if len(duplicate_cols) > 0:
    print(f"   🗑️ Loại bỏ {len(duplicate_cols)} cột trùng lặp")
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

# Xử lý null values
print(f"   �� Xử lý giá trị null...")
for col in merged_df.columns:
    if merged_df[col].dtype == 'object':
        merged_df[col] = merged_df[col].fillna('unknown')
    elif merged_df[col].dtype in ['float64', 'int64']:
        merged_df[col] = merged_df[col].fillna(0)

# 5. Lưu kết quả
output_file = f"{output_folder}/merged_user_data.csv"
merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"\n�� HOÀN THÀNH MERGE/JOIN!")
print(f"📁 File kết quả: {output_file}")
print(f"📊 Kích thước cuối cùng: {len(merged_df):,} dòng × {len(merged_df.columns)} cột")
print(f"�� Kích thước file: {os.path.getsize(output_file) / (1024*1024):.2f} MB")

# 6. Thống kê chi tiết
print(f"\n📈 THỐNG KÊ CHI TIẾT:")
print("=" * 60)
print(f"🔑 Key chính: user_pseudo_id")
print(f"📅 Cột thời gian: {len([col for col in merged_df.columns if 'date' in col.lower()])} cột")
print(f"💰 Cột doanh thu: {len([col for col in merged_df.columns if 'revenue' in col.lower() or 'value' in col.lower()])} cột")
print(f"🎮 Cột game: {len([col for col in merged_df.columns if 'level' in col.lower() or 'win' in col.lower() or 'tut' in col.lower()])} cột")

# Hiển thị 5 dòng đầu
print(f"\n👀 Dữ liệu mẫu (5 dòng đầu):")
print(merged_df.head().to_string())

# Lưu thống kê cột
columns_info = []
for col in merged_df.columns:
    null_count = merged_df[col].isnull().sum()
    null_pct = round((null_count / len(merged_df)) * 100, 2)
    columns_info.append({
        "Cột": col,
        "Kiểu dữ liệu": str(merged_df[col].dtype),
        "Số giá trị null": null_count,
        "% Null": null_pct
    })

columns_df = pd.DataFrame(columns_info)
columns_df.to_csv(f"{output_folder}/merged_columns_info.csv", index=False, encoding='utf-8-sig')
print(f"\n�� Thông tin cột đã lưu: {output_folder}/merged_columns_info.csv")