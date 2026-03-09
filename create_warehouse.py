import pandas as pd
import os
from datetime import datetime

# Thư mục chứa CSV đã làm sạch
cleaned_folder = "Dataset/cleaned"

# Đọc tất cả file CSV đã làm sạch
print("🔄 Đang đọc dữ liệu từ các file đã làm sạch...")

# Đọc từng file
df_ad_action = pd.read_csv(f"{cleaned_folder}/AD_ACTION.csv", low_memory=False)
df_ad_revenue = pd.read_csv(f"{cleaned_folder}/AD_REVENUE.csv", low_memory=False)
df_coin_gem = pd.read_csv(f"{cleaned_folder}/COIN_GEM.csv", low_memory=False)
df_dau = pd.read_csv(f"{cleaned_folder}/DAU.csv", low_memory=False)
df_engagement = pd.read_csv(f"{cleaned_folder}/ENGAGEMENT.csv", low_memory=False)
df_first_open = pd.read_csv(f"{cleaned_folder}/FIRST_OPEN.csv", low_memory=False)
df_revenue = pd.read_csv(f"{cleaned_folder}/REVENUE.csv", low_memory=False)
df_start_win_lose = pd.read_csv(f"{cleaned_folder}/START_WIN_LOSE.csv", low_memory=False)
df_tutorial = pd.read_csv(f"{cleaned_folder}/TUTORIAL.csv", low_memory=False)
df_uninstall = pd.read_csv(f"{cleaned_folder}/UNINSTALL_USER.csv", low_memory=False)

print("✅ Đã đọc xong tất cả file dữ liệu")

# ============================================================================
# TẠO CÁC BẢNG DIMENSION
# ============================================================================

print("\n��️ Đang tạo các bảng DIMENSION...")

# 1. DIM_USER - Thông tin người dùng
print("�� Tạo DIM_USER...")
# Sử dụng parse_event_date thay vì install_date
dim_user = df_first_open[['user_pseudo_id', 'parse_event_date', 'device', 'version', 'package_name', 'country']].copy()
dim_user = dim_user.rename(columns={'user_pseudo_id': 'user_id', 'parse_event_date': 'install_date'})
dim_user = dim_user.drop_duplicates(subset=['user_id'])
dim_user['user_key'] = range(1, len(dim_user) + 1)
dim_user = dim_user[['user_key', 'user_id', 'install_date', 'device', 'version', 'package_name', 'country']]

# 2. DIM_TIME - Thông tin thời gian
print("⏰ Tạo DIM_TIME...")
# Thu thập tất cả ngày từ các bảng
all_dates = set()
for df in [df_ad_action, df_ad_revenue, df_coin_gem, df_dau, df_engagement, 
           df_first_open, df_revenue, df_start_win_lose, df_tutorial, df_uninstall]:
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    for col in date_cols:
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            valid_dates = df[col].dropna()
            all_dates.update(valid_dates.dt.date)
        except:
            pass

# Tạo bảng thời gian
dim_time = pd.DataFrame({'date': sorted(list(all_dates))})
dim_time['date_key'] = range(1, len(dim_time) + 1)
dim_time['day_of_week'] = pd.to_datetime(dim_time['date']).dt.day_name()
dim_time['month'] = pd.to_datetime(dim_time['date']).dt.month
dim_time['quarter'] = pd.to_datetime(dim_time['date']).dt.quarter
dim_time['year'] = pd.to_datetime(dim_time['date']).dt.year
dim_time = dim_time[['date_key', 'date', 'day_of_week', 'month', 'quarter', 'year']]

# 3. DIM_LOCATION - Thông tin địa lý
print("🌍 Tạo DIM_LOCATION...")
location_data = df_first_open[['country']].dropna().drop_duplicates()
dim_location = pd.DataFrame({'country': location_data['country'].unique()})
dim_location['location_key'] = range(1, len(dim_location) + 1)
dim_location = dim_location[['location_key', 'country']]

# 4. DIM_DEVICE - Thông tin thiết bị
print("📱 Tạo DIM_DEVICE...")
device_data = df_first_open[['device', 'version', 'package_name']].dropna().drop_duplicates()
dim_device = pd.DataFrame(device_data)
dim_device['device_key'] = range(1, len(dim_device) + 1)
dim_device = dim_device[['device_key', 'device', 'version', 'package_name']]

# 5. DIM_ACTION - Thông tin hành động
print("🎯 Tạo DIM_ACTION...")
# Kiểm tra xem có cột action không
if 'action' in df_ad_action.columns:
    action_data = df_ad_action[['action', 'type', 'SOURCE']].dropna().drop_duplicates()
    dim_action = pd.DataFrame(action_data)
    dim_action['action_key'] = range(1, len(dim_action) + 1)
    dim_action = dim_action[['action_key', 'action', 'type', 'SOURCE']]
else:
    # Tạo bảng action mặc định nếu không có
    dim_action = pd.DataFrame({
        'action_key': [1],
        'action': ['unknown'],
        'type': ['unknown'],
        'SOURCE': ['unknown']
    })

# ============================================================================
# TẠO BẢNG FACT CHÍNH
# ============================================================================

print("\n Đang tạo bảng FACT chính...")

# Tạo bảng FACT_USER_ACTIVITY
fact_user_activity = []

print("🔄 Xử lý dữ liệu AD_ACTION...")
for _, row in df_ad_action.iterrows():
    try:
        # Tìm user_key
        user_key = dim_user[dim_user['user_id'] == row['user_pseudo_id']]['user_key'].iloc[0] if len(dim_user[dim_user['user_id'] == row['user_pseudo_id']]) > 0 else None
        
        # Tìm date_key - sử dụng event_date hoặc parse_event_date
        event_date = None
        if 'event_date' in row:
            event_date = pd.to_datetime(row['event_date'], errors='coerce')
        elif 'parse_event_date' in row:
            event_date = pd.to_datetime(row['parse_event_date'], errors='coerce')
            
        date_key = None
        if event_date and not pd.isna(event_date):
            matching_dates = dim_time[dim_time['date'] == event_date.date()]
            if len(matching_dates) > 0:
                date_key = matching_dates['date_key'].iloc[0]
        
        # Tìm action_key
        action_key = None
        if 'action' in row and 'type' in row and 'SOURCE' in row:
            matching_actions = dim_action[(dim_action['action'] == row['action']) & 
                                        (dim_action['type'] == row['type']) & 
                                        (dim_action['SOURCE'] == row['SOURCE'])]
            if len(matching_actions) > 0:
                action_key = matching_actions['action_key'].iloc[0]
        
        fact_user_activity.append({
            'user_key': user_key,
            'date_key': date_key,
            'action_key': action_key,
            'advertising_id': row.get('advertising_id', 'unknown'),
            'level_reach': row.get('level_reach', 0),
            'days_playing': row.get('days_playing', 0),
            'days_since_install': row.get('days_since_install', 0),
            'status': row.get('status', 'unknown'),
            'location': row.get('location', 'unknown')
        })
    except Exception as e:
        print(f"   ⚠️ Lỗi xử lý dòng AD_ACTION: {e}")
        continue

print("🔄 Xử lý dữ liệu AD_REVENUE...")
for _, row in df_ad_revenue.iterrows():
    try:
        user_key = dim_user[dim_user['user_id'] == row['user_pseudo_id']]['user_key'].iloc[0] if len(dim_user[dim_user['user_id'] == row['user_pseudo_id']]) > 0 else None
        
        # Tìm date_key
        event_date = None
        if 'event_date' in row:
            event_date = pd.to_datetime(row['event_date'], errors='coerce')
        elif 'parse_event_date' in row:
            event_date = pd.to_datetime(row['parse_event_date'], errors='coerce')
            
        date_key = None
        if event_date and not pd.isna(event_date):
            matching_dates = dim_time[dim_time['date'] == event_date.date()]
            if len(matching_dates) > 0:
                date_key = matching_dates['date_key'].iloc[0]
        
        fact_user_activity.append({
            'user_key': user_key,
            'date_key': date_key,
            'action_key': None,
            'advertising_id': row.get('advertising_id', 'unknown'),
            'level_reach': row.get('level_reach', 0),
            'days_playing': row.get('days_playing', 0),
            'days_since_install': row.get('days_since_install', 0),
            'status': 'revenue',
            'location': row.get('location', 'unknown'),
            'revenue_amount': row.get('revenue', 0)
        })
    except Exception as e:
        print(f"   ⚠️ Lỗi xử lý dòng AD_REVENUE: {e}")
        continue

# Chuyển thành DataFrame
fact_user_activity = pd.DataFrame(fact_user_activity)

# ============================================================================
# LƯU CÁC BẢNG
# ============================================================================

print("\n💾 Đang lưu các bảng...")

# Tạo thư mục output
output_folder = "Dataset/warehouse"
os.makedirs(output_folder, exist_ok=True)

# Lưu các bảng DIMENSION
dim_user.to_csv(f"{output_folder}/DIM_USER.csv", index=False, encoding='utf-8-sig')
dim_time.to_csv(f"{output_folder}/DIM_TIME.csv", index=False, encoding='utf-8-sig')
dim_location.to_csv(f"{output_folder}/DIM_LOCATION.csv", index=False, encoding='utf-8-sig')
dim_device.to_csv(f"{output_folder}/DIM_DEVICE.csv", index=False, encoding='utf-8-sig')
dim_action.to_csv(f"{output_folder}/DIM_ACTION.csv", index=False, encoding='utf-8-sig')

# Lưu bảng FACT
fact_user_activity.to_csv(f"{output_folder}/FACT_USER_ACTIVITY.csv", index=False, encoding='utf-8-sig')

# ============================================================================
# HIỂN THỊ THÔNG TIN
# ============================================================================

print("\n" + "="*80)
print("️ HOÀN THÀNH XÂY DỰNG DATA WAREHOUSE")
print("="*80)

print(f"\n DIM_USER: {len(dim_user)} dòng, {dim_user.shape[1]} cột")
print(f"⏰ DIM_TIME: {len(dim_time)} dòng, {dim_time.shape[1]} cột")
print(f" DIM_LOCATION: {len(dim_location)} dòng, {dim_location.shape[1]} cột")
print(f" DIM_DEVICE: {len(dim_device)} dòng, {dim_device.shape[1]} cột")
print(f" DIM_ACTION: {len(dim_action)} dòng, {dim_action.shape[1]} cột")
print(f" FACT_USER_ACTIVITY: {len(fact_user_activity)} dòng, {fact_user_activity.shape[1]} cột")

print(f"\n📁 Tất cả bảng đã được lưu tại: {output_folder}")

# Hiển thị mẫu dữ liệu
print("\n" + "="*50)
print("📋 MẪU DỮ LIỆU DIM_USER:")
print("="*50)
print(dim_user.head())

print("\n" + "="*50)
print("📋 MẪU DỮ LIỆU FACT_USER_ACTIVITY:")
print("="*50)
print(fact_user_activity.head())