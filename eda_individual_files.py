import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# Thiết lập style cho biểu đồ
plt.style.use('default')
plt.rcParams['font.size'] = 10

print("🚀 Bắt đầu Phân tích khám phá (EDA) với 10 file CSV riêng lẻ...")

# Tạo thư mục output
import os
output_folder = "Dataset/analysis"
os.makedirs(output_folder, exist_ok=True)

# 1. TÍNH THỐNG KÊ CƠ BẢN CHO TỪNG FILE
print("\n" + "="*80)
print(" TÍNH THỐNG KÊ CƠ BẢN CHO TỪNG FILE")
print("="*80)

# Danh sách file CSV
files = {
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

# Lưu trữ thống kê tổng hợp
all_stats = []
all_data = {}

def find_real_date_column(df):
    """Tìm cột thời gian thực tế thay vì timestamp Unix"""
    date_columns = [col for col in df.columns if 'date' in col.lower()]
    
    for col in date_columns:
        try:
            # Chuyển đổi sang datetime
            df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Kiểm tra xem có phải timestamp Unix không
            sample_dates = df[col].dropna().head(1000)
            if len(sample_dates) > 0:
                # Nếu tất cả đều là 1970-01-01, bỏ qua cột này
                if len(sample_dates.dt.year.unique()) == 1 and sample_dates.dt.year.unique()[0] == 1970:
                    print(f"   ⚠️ {col}: Timestamp Unix (1970), bỏ qua")
                    continue
                else:
                    min_date = sample_dates.min()
                    max_date = sample_dates.max()
                    print(f"   ✅ {col}: Ngày từ {min_date.strftime('%Y-%m-%d')} đến {max_date.strftime('%Y-%m-%d')}")
                    return col
        except Exception as e:
            print(f"   ❌ Lỗi với cột {col}: {e}")
            continue
    
    print("   ⚠️ Không tìm thấy cột thời gian thực tế")
    return None

for name, path in files.items():
    try:
        print(f"\n🔄 Đang xử lý: {name}")
        df = pd.read_csv(path, low_memory=False)
        all_data[name] = df
        
        # Tìm cột thời gian thực tế
        real_date_col = find_real_date_column(df)
        
        # Thống kê cơ bản
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_columns) > 0:
            stats = df[numeric_columns].describe()
            print(f"   Số dòng: {len(df):,}, Số cột: {df.shape[1]}")
            print(f"   🔢 Cột số: {len(numeric_columns)}")
            
            # Lưu thống kê
            stats.to_csv(f"{output_folder}/stats_{name}.csv")
            
            # Thống kê tổng hợp
            all_stats.append({
                'File': name,
                'Số dòng': len(df),
                'Số cột': df.shape[1],
                'Cột số': len(numeric_columns),
                'Cột thời gian thực': 1 if real_date_col else 0,
                'Kích thước (MB)': round(os.path.getsize(path) / (1024*1024), 2)
            })
        else:
            print(f"   ⚠️ Không có cột số trong {name}")
            
    except Exception as e:
        print(f"   ❌ Lỗi khi xử lý {name}: {str(e)}")

# Lưu thống kê tổng hợp
if all_stats:
    stats_summary = pd.DataFrame(all_stats)
    stats_summary.to_csv(f"{output_folder}/summary_statistics.csv", index=False)
    print(f"\n Đã lưu thống kê tổng hợp: {output_folder}/summary_statistics.csv")

# 2. VẼ BIỂU ĐỒ THEO THỜI GIAN CHO TỪNG FILE
print("\n" + "="*80)
print(" VẼ BIỂU ĐỒ THEO THỜI GIAN CHO TỪNG FILE")
print("="*80)

for name, df in all_data.items():
    try:
        # Tìm cột thời gian thực tế
        real_date_col = find_real_date_column(df)
        
        if real_date_col:
            # Chuẩn bị dữ liệu theo ngày
            df[real_date_col] = pd.to_datetime(df[real_date_col], errors='coerce')
            
            # Loại bỏ ngày null
            df_clean = df.dropna(subset=[real_date_col])
            
            if len(df_clean) > 0:
                # Nhóm dữ liệu theo ngày
                if 'user_pseudo_id' in df_clean.columns:
                    daily_data = df_clean.groupby(df_clean[real_date_col].dt.date)['user_pseudo_id'].nunique().reset_index()
                    daily_data.columns = ['date', 'daily_users']
                    y_label = 'Số người dùng unique'
                else:
                    daily_data = df_clean.groupby(df_clean[real_date_col].dt.date).size().reset_index()
                    daily_data.columns = ['date', 'daily_count']
                    y_label = 'Số lượng sự kiện'
                
                # Chỉ vẽ nếu có nhiều hơn 1 ngày
                if len(daily_data) > 1:
                    # Sắp xếp theo ngày
                    daily_data = daily_data.sort_values('date')
                    
                    # Vẽ biểu đồ
                    plt.figure(figsize=(12, 6))
                    plt.plot(daily_data['date'], daily_data.iloc[:, 1], marker='o', linewidth=2, markersize=4, color='blue')
                    plt.title(f'�� {name} - {y_label} theo ngày', fontsize=14, fontweight='bold')
                    plt.xlabel('Ngày')
                    plt.ylabel(y_label)
                    plt.xticks(rotation=45)
                    plt.grid(True, alpha=0.3)
                    plt.tight_layout()
                    
                    # Lưu biểu đồ
                    plt.savefig(f'{output_folder}/time_series_{name}.png', dpi=300, bbox_inches='tight')
                    plt.show()
                    
                    print(f"   ✅ Đã vẽ biểu đồ cho {name} với {len(daily_data)} ngày")
                else:
                    print(f"   ⚠️ {name}: Chỉ có {len(daily_data)} ngày, không vẽ biểu đồ")
            else:
                print(f"   ⚠️ {name}: Không có dữ liệu thời gian hợp lệ")
        else:
            print(f"   ⚠️ {name}: Không có cột thời gian thực tế")
            
    except Exception as e:
        print(f"   ❌ Lỗi khi vẽ biểu đồ {name}: {str(e)}")

# 3. PHÂN TÍCH COHORT (Sử dụng FIRST_OPEN làm bảng chính)
print("\n" + "="*80)
print(" PHÂN TÍCH COHORT SỬ DỤNG FIRST_OPEN")
print("="*80)

if 'FIRST_OPEN' in all_data:
    try:
        first_open_df = all_data['FIRST_OPEN'].copy()
        
        # Tìm cột thời gian thực tế
        real_date_col = find_real_date_column(first_open_df)
        
        if real_date_col:
            first_open_df[real_date_col] = pd.to_datetime(first_open_df[real_date_col], errors='coerce')
            first_open_df = first_open_df.dropna(subset=[real_date_col])
            
            if len(first_open_df) > 0:
                # Tạo cohort data theo tháng
                first_open_df['install_month'] = first_open_df[real_date_col].dt.to_period('M')
                
                cohort_data = first_open_df.groupby('install_month')['user_pseudo_id'].nunique().reset_index()
                cohort_data.columns = ['Tháng cài đặt', 'Số người dùng mới']
                
                if len(cohort_data) > 1:
                    # Vẽ biểu đồ cohort
                    plt.figure(figsize=(12, 6))
                    bars = plt.bar(range(len(cohort_data)), cohort_data['Số người dùng mới'], color='orange', alpha=0.7)
                    plt.title('🔥 Cohort Analysis - Người dùng mới theo tháng', fontsize=14, fontweight='bold')
                    plt.xlabel('Tháng')
                    plt.ylabel('Số người dùng mới')
                    plt.xticks(range(len(cohort_data)), [str(x) for x in cohort_data['Tháng cài đặt']], rotation=45)
                    plt.grid(True, alpha=0.3)
                    plt.tight_layout()
                    
                    plt.savefig(f'{output_folder}/cohort_analysis.png', dpi=300, bbox_inches='tight')
                    plt.show()
                    
                    # Lưu cohort data
                    cohort_data.to_csv(f"{output_folder}/cohort_data.csv", index=False)
                    print("   ✅ Đã hoàn thành phân tích cohort")
                else:
                    print("   ⚠️ Chỉ có 1 tháng dữ liệu, không thể phân tích cohort")
            else:
                print("   ⚠️ Không có dữ liệu thời gian hợp lệ cho cohort")
        else:
            print("   ⚠️ Không có cột thời gian thực tế cho cohort")
            
    except Exception as e:
        print(f"   ❌ Lỗi khi phân tích cohort: {str(e)}")

# 4. 📌 PHÁT HIỆN OUTLIER CHO TỪNG FILE
print("\n" + "="*80)
print("📌 PHÁT HIỆN OUTLIER CHO TỪNG FILE")
print("="*80)

all_outliers = []

for name, df in all_data.items():
    try:
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_columns:
            if col in df.columns and df[col].notna().sum() > 0:
                # Phát hiện outlier bằng IQR method
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
                outlier_count = len(outliers)
                
                if outlier_count > 0:
                    outlier_pct = round((outlier_count / len(df)) * 100, 2)
                    
                    all_outliers.append({
                        'File': name,
                        'Cột': col,
                        'Số outlier': outlier_count,
                        '% Outlier': outlier_pct,
                        'Giá trị min outlier': outliers[col].min(),
                        'Giá trị max outlier': outliers[col].max()
                    })
                    
                    # Vẽ boxplot cho cột có outlier
                    if outlier_count > len(df) * 0.01:  # Chỉ vẽ nếu outlier > 1%
                        plt.figure(figsize=(10, 6))
                        plt.boxplot(df[col].dropna())
                        plt.title(f'📦 {name} - {col} (Outlier: {outlier_count})', fontsize=12)
                        plt.ylabel('Giá trị')
                        plt.grid(True, alpha=0.3)
                        plt.savefig(f'{output_folder}/outlier_{name}_{col}.png', dpi=300, bbox_inches='tight')
                        plt.show()
        
        print(f"   ✅ Đã kiểm tra outlier cho {name}")
        
    except Exception as e:
        print(f"   ❌ Lỗi khi kiểm tra outlier {name}: {str(e)}")

# Lưu thông tin outlier
if all_outliers:
    outlier_df = pd.DataFrame(all_outliers)
    outlier_df.to_csv(f"{output_folder}/all_outliers.csv", index=False)
    print(f"\n💾 Đã lưu thông tin outlier: {output_folder}/all_outliers.csv")

# 5. HEATMAP TƯƠNG QUAN CHO TỪNG FILE
print("\n" + "="*80)
print(" HEATMAP TƯƠNG QUAN CHO TỪNG FILE")
print("="*80)

for name, df in all_data.items():
    try:
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_columns) > 1:
            # Chọn cột có đủ dữ liệu
            valid_numeric = [col for col in numeric_columns if df[col].notna().sum() > len(df) * 0.1]
            
            if len(valid_numeric) > 1:
                # Tính correlation matrix
                correlation_matrix = df[valid_numeric].corr()
                
                # Vẽ heatmap đơn giản với matplotlib
                plt.figure(figsize=(10, 8))
                im = plt.imshow(correlation_matrix, cmap='RdYlBu', aspect='auto')
                plt.colorbar(im, label='Hệ số tương quan')
                
                # Thêm text cho từng ô
                for i in range(len(correlation_matrix.columns)):
                    for j in range(len(correlation_matrix.columns)):
                        text = plt.text(j, i, f'{correlation_matrix.iloc[i, j]:.2f}',
                                      ha="center", va="center", color="black", fontsize=8)
                
                plt.title(f' {name} - Heatmap Tương Quan', fontsize=14, fontweight='bold')
                plt.xticks(range(len(correlation_matrix.columns)), correlation_matrix.columns, rotation=45)
                plt.yticks(range(len(correlation_matrix.columns)), correlation_matrix.columns)
                plt.tight_layout()
                plt.savefig(f'{output_folder}/correlation_{name}.png', dpi=300, bbox_inches='tight')
                plt.show()
                
                # Lưu correlation matrix
                correlation_matrix.to_csv(f"{output_folder}/correlation_{name}.csv")
                
                print(f"   ✅ Đã vẽ heatmap tương quan cho {name}")
            else:
                print(f"   ⚠️ {name}: Không đủ cột số để vẽ correlation")
        else:
            print(f"   ⚠️ {name}: Chỉ có {len(numeric_columns)} cột số")
            
    except Exception as e:
        print(f"   ❌ Lỗi khi vẽ correlation {name}: {str(e)}")

# 6. PHÂN TÍCH ĐẶC BIỆT CHO TỪNG LOẠI DỮ LIỆU
print("\n" + "="*80)
print(" PHÂN TÍCH ĐẶC BIỆT CHO TỪNG LOẠI DỮ LIỆU")
print("="*80)

# Phân tích REVENUE
if 'REVENUE' in all_data:
    try:
        revenue_df = all_data['REVENUE']
        if 'revenue' in revenue_df.columns:
            # Loại bỏ giá trị 0 và null
            revenue_data = revenue_df['revenue'].dropna()
            revenue_data = revenue_data[revenue_data > 0]
            
            if len(revenue_data) > 0:
                plt.figure(figsize=(10, 6))
                plt.hist(revenue_data, bins=20, alpha=0.7, color='green')
                plt.title('💰 Phân Phối Doanh Thu (REVENUE)', fontsize=14, fontweight='bold')
                plt.xlabel('Doanh thu')
                plt.ylabel('Tần suất')
                plt.grid(True, alpha=0.3)
                plt.savefig(f'{output_folder}/revenue_distribution.png', dpi=300, bbox_inches='tight')
                plt.show()
                print("   ✅ Đã phân tích phân phối doanh thu")
            else:
                print("   ⚠️ Không có dữ liệu doanh thu > 0")
    except Exception as e:
        print(f"   ❌ Lỗi khi phân tích REVENUE: {str(e)}")

# Phân tích ENGAGEMENT
if 'ENGAGEMENT' in all_data:
    try:
        engagement_df = all_data['ENGAGEMENT']
        if 'engagement_time_sec' in engagement_df.columns:
            # Loại bỏ giá trị 0 và null
            engagement_data = engagement_df['engagement_time_sec'].dropna()
            engagement_data = engagement_data[engagement_data > 0]
            
            if len(engagement_data) > 0:
                plt.figure(figsize=(10, 6))
                plt.hist(engagement_data, bins=30, alpha=0.7, color='orange')
                plt.title('⏱️ Phân Phối Thời Gian Tương Tác (ENGAGEMENT)', fontsize=14, fontweight='bold')
                plt.xlabel('Thời gian (giây)')
                plt.ylabel('Tần suất')
                plt.grid(True, alpha=0.3)
                plt.savefig(f'{output_folder}/engagement_distribution.png', dpi=300, bbox_inches='tight')
                plt.show()
                print("   ✅ Đã phân tích phân phối thời gian tương tác")
            else:
                print("   ⚠️ Không có dữ liệu thời gian tương tác > 0")
    except Exception as e:
        print(f"   ❌ Lỗi khi phân tích ENGAGEMENT: {str(e)}")

# 7. TỔNG KẾT EDA
print("\n" + "="*80)
print(" TỔNG KẾT PHÂN TÍCH EDA")
print("="*80)

print("✅ Đã hoàn thành các phân tích cho 10 file CSV:")
print("   📊 Thống kê cơ bản cho từng file")
print("   📈 Biểu đồ theo thời gian cho từng file (nếu có dữ liệu thời gian thực)")
print("   Phân tích cohort (FIRST_OPEN)")
print("   📌 Phát hiện outlier cho từng file")
print("   Heatmap tương quan cho từng file")
print("   Phân tích đặc biệt (Revenue, Engagement)")

print(f"\n📁 Các file đã tạo trong {output_folder}:")
print("   summary_statistics.csv - Thống kê tổng hợp")
print("   📈 time_series_[tên_file].png - Biểu đồ theo thời gian (nếu có)")
print("   🔥 cohort_analysis.png - Phân tích cohort (nếu có)")
print("   all_outliers.csv - Thông tin outlier")
print("   🔥 correlation_[tên_file].png - Heatmap tương quan")
print("   💰 revenue_distribution.png - Phân phối doanh thu (nếu có)")
print("   ⏱️ engagement_distribution.png - Phân phối thời gian tương tác (nếu có)")

print(f"\n🎯 Kết quả chính:")
print(f"   Tổng số file đã xử lý: {len(all_data)}")
print(f"   Tổng số dòng dữ liệu: {sum([len(df) for df in all_data.values()]):,}")
print(f"   Outlier phát hiện: {len(all_outliers)} trường hợp")

print("\n EDA hoàn thành! Mỗi file CSV đã được phân tích riêng biệt.")
print("💡 Bạn có thể xem từng biểu đồ và file để hiểu sâu về từng loại dữ liệu.")
print("⚠️ Lưu ý: Một số file có thể không có dữ liệu thời gian thực tế (chỉ có timestamp Unix)")