import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

print("�� Bắt đầu Giai đoạn 4 - Phân tích theo đề tài...")

# Tạo thư mục output
import os
output_folder = "Dataset/analysis_stage4"
os.makedirs(output_folder, exist_ok=True)

# ============================================================================
# 4.1 PHÂN TÍCH HÀNH VI NGƯỜI CHƠI
# ============================================================================
print("\n" + "="*80)
print("4.1 PHÂN TÍCH HÀNH VI NGƯỜI CHƠI")
print("="*80)

def analyze_player_behavior():
    """Phân tích hành vi người chơi"""
    
    print("🔄 Đang đọc dữ liệu...")
    
    # Đọc các file cần thiết
    tutorial_df = pd.read_csv('Dataset/cleaned/TUTORIAL.csv', low_memory=False)
    start_win_lose_df = pd.read_csv('Dataset/cleaned/START_WIN_LOSE.csv', low_memory=False)
    coin_gem_df = pd.read_csv('Dataset/cleaned/COIN_GEM.csv', low_memory=False)
    revenue_df = pd.read_csv('Dataset/cleaned/REVENUE.csv', low_memory=False)
    engagement_df = pd.read_csv('Dataset/cleaned/ENGAGEMENT.csv', low_memory=False)
    
    print(f"✅ Đã đọc dữ liệu từ {len(tutorial_df)} dòng tutorial")
    
    # 16. XÂY DỰNG FUNNEL HÀNH VI
    print("\n�� Xây dựng funnel hành vi...")
    
    # Tính số lượng user ở mỗi bước
    funnel_data = {}
    
    # Bước 1: Tutorial
    if 'user_pseudo_id' in tutorial_df.columns:
    tutorial_users = tutorial_df['user_pseudo_id'].nunique()
    funnel_data['Tutorial'] = tutorial_users
        print(f"📚 Tutorial: {tutorial_users:,} users")
    
    # Bước 2: Win/Lose (game events)
    if 'user_pseudo_id' in start_win_lose_df.columns:
    game_users = start_win_lose_df['user_pseudo_id'].nunique()
        funnel_data['Game_Events'] = game_users
        print(f"�� Game Events: {game_users:,} users")
    
    # Bước 3: Coin usage
    if 'user_pseudo_id' in coin_gem_df.columns:
    coin_users = coin_gem_df['user_pseudo_id'].nunique()
    funnel_data['Coin_Usage'] = coin_users
        print(f"💰 Coin Usage: {coin_users:,} users")
    
    # Bước 4: Revenue
    if 'user_pseudo_id' in revenue_df.columns:
    revenue_users = revenue_df['user_pseudo_id'].nunique()
    funnel_data['Revenue'] = revenue_users
        print(f"💎 Revenue: {revenue_users:,} users")
    
    # Vẽ funnel chart
    if len(funnel_data) > 0:
    plt.figure(figsize=(10, 8))
    stages = list(funnel_data.keys())
    values = list(funnel_data.values())
    
    plt.bar(stages, values, color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4'])
        plt.title('Funnel Hành Vi Người Chơi', fontsize=16, fontweight='bold')
        plt.ylabel('Số lượng User', fontsize=12)
        plt.xlabel('Giai đoạn', fontsize=12)
        
        # Thêm số liệu trên mỗi cột
        for i, v in enumerate(values):
            plt.text(i, v + max(values)*0.01, f'{v:,}', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
        plt.savefig(f'{output_folder}/funnel_behavior.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"📊 Đã lưu funnel chart: {output_folder}/funnel_behavior.png")
    
    # 17. PHÂN CỤM NGƯỜI CHƠI (CLUSTERING)
    print("\n�� Phân cụm người chơi...")
    
    # Tạo user features đơn giản
    user_features = {}
    
    # Tutorial completion
    if 'user_pseudo_id' in tutorial_df.columns:
        tutorial_stats = tutorial_df.groupby('user_pseudo_id').size().reset_index(name='tutorial_count')
        user_features['tutorial'] = tutorial_stats
    
    # Game events
    if 'user_pseudo_id' in start_win_lose_df.columns:
        game_stats = start_win_lose_df.groupby('user_pseudo_id').size().reset_index(name='game_count')
        user_features['game'] = game_stats
    
    # Coin usage
    if 'user_pseudo_id' in coin_gem_df.columns:
        coin_stats = coin_gem_df.groupby('user_pseudo_id').size().reset_index(name='coin_count')
        user_features['coin'] = coin_stats
    
    # Merge tất cả features
    if len(user_features) > 0:
        master_df = None
        for key, df in user_features.items():
            if master_df is None:
                master_df = df
            else:
                master_df = master_df.merge(df, on='user_pseudo_id', how='outer')
        
        # Fill null bằng 0
        master_df = master_df.fillna(0)
        
        # Chọn cột numeric để clustering
        numeric_cols = master_df.select_dtypes(include=[np.number]).columns.tolist()
        if 'user_pseudo_id' in numeric_cols:
            numeric_cols.remove('user_pseudo_id')
        
        if len(numeric_cols) > 0:
    # Chuẩn hóa dữ liệu
    scaler = StandardScaler()
            scaled_data = scaler.fit_transform(master_df[numeric_cols])
            
            # K-means clustering
            kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
            master_df['cluster'] = kmeans.fit_predict(scaled_data)
            
            # Phân tích cluster
            cluster_analysis = master_df.groupby('cluster')[numeric_cols].mean().round(2)
            cluster_analysis['user_count'] = master_df['cluster'].value_counts().sort_index()
            
            print("�� Kết quả clustering:")
    print(cluster_analysis)
    
            # Lưu kết quả
            cluster_analysis.to_csv(f'{output_folder}/player_clusters.csv')
            master_df.to_csv(f'{output_folder}/user_features_with_clusters.csv', index=False)
            
            print(f"✅ Đã lưu kết quả clustering: {output_folder}/player_clusters.csv")
    
    # 18. PHÂN TÍCH PATTERN GIỮ CHÂN vs RỜI GAME
    print("\n📌 Phân tích pattern giữ chân vs rời game...")
    
    # Tạo retention analysis đơn giản
    retention_data = {}
    
    if 'user_pseudo_id' in engagement_df.columns:
        # Tính số ngày unique mỗi user
        user_days = engagement_df.groupby('user_pseudo_id')['event_date'].nunique().reset_index(name='active_days')
        
        # Phân loại user
        user_days['user_type'] = pd.cut(user_days['active_days'], 
                                       bins=[0, 1, 3, 7, float('inf')], 
                                       labels=['Churn', 'Low', 'Medium', 'High'])
        
        retention_summary = user_days['user_type'].value_counts()
        retention_data = retention_summary.to_dict()
        
        print("📊 Phân tích retention:")
        for user_type, count in retention_data.items():
            percentage = (count / len(user_days)) * 100
            print(f"  {user_type}: {count:,} users ({percentage:.1f}%)")
        
        # Lưu kết quả
        user_days.to_csv(f'{output_folder}/user_retention_analysis.csv', index=False)
        print(f"✅ Đã lưu retention analysis: {output_folder}/user_retention_analysis.csv")
    
    return user_features, cluster_analysis if 'cluster_analysis' in locals() else None, retention_data

# ============================================================================
# 4.2 CHURN PREDICTION
# ============================================================================
print("\n" + "="*80)
print("4.2 CHURN PREDICTION")
print("="*80)

def analyze_churn_prediction():
    """Phân tích dự đoán churn"""
    
    print("🔄 Đang đọc dữ liệu churn...")
    
    # Đọc dữ liệu
    uninstall_df = pd.read_csv('Dataset/cleaned/UNINSTALL_USER.csv', low_memory=False)
    engagement_df = pd.read_csv('Dataset/cleaned/ENGAGEMENT.csv', low_memory=False)
    
    print(f"✅ Đã đọc {len(uninstall_df)} dòng uninstall")
    
    # 19. LABEL CHURN
    print("\n🏷 Label churn...")
    
    if 'user_pseudo_id' in uninstall_df.columns:
    churned_users = set(uninstall_df['user_pseudo_id'].unique())
        print(f"📊 Số user đã churn: {len(churned_users):,}")
        
        # Tạo churn label cho tất cả user
        all_users = set(engagement_df['user_pseudo_id'].unique())
        churn_labels = pd.DataFrame({
            'user_pseudo_id': list(all_users),
            'is_churned': [1 if user in churned_users else 0 for user in all_users]
        })
        
        print(f"�� Tỷ lệ churn: {(len(churned_users)/len(all_users)*100):.2f}%")
    
    # 20. FEATURE ENGINEERING
    print("\n🛠 Feature engineering...")
    
    if 'user_pseudo_id' in engagement_df.columns:
    # Tính engagement features
    engagement_features = engagement_df.groupby('user_pseudo_id').agg({
            'event_date': ['nunique', 'count'],  # Số ngày active, tổng events
        }).round(2)
        
        # Flatten column names
        engagement_features.columns = ['active_days', 'total_events']
        engagement_features = engagement_features.reset_index()
        
        # Tính thêm features
        engagement_features['avg_events_per_day'] = (engagement_features['total_events'] / 
                                                   engagement_features['active_days']).fillna(0)
        
        # Merge với churn labels
        if 'churn_labels' in locals():
            final_features = churn_labels.merge(engagement_features, on='user_pseudo_id', how='left')
            final_features = final_features.fillna(0)
            
            print(f"✅ Đã tạo {len(final_features)} features cho {len(final_features)} users")
            
            # Lưu features
            final_features.to_csv(f'{output_folder}/churn_prediction_features.csv', index=False)
            print(f"�� Đã lưu features: {output_folder}/churn_prediction_features.csv")
    
    # 21. TRAIN MÔ HÌNH (đơn giản)
    print("\n🤖 Training mô hình...")
    
    if 'final_features' in locals() and len(final_features) > 0:
        # Chọn features numeric
        feature_cols = final_features.select_dtypes(include=[np.number]).columns.tolist()
        if 'is_churned' in feature_cols:
            feature_cols.remove('is_churned')
        if 'user_pseudo_id' in feature_cols:
            feature_cols.remove('user_pseudo_id')
        
        if len(feature_cols) > 0:
            X = final_features[feature_cols]
            y = final_features['is_churned']
    
    # Split data
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            # Train Logistic Regression
            lr_model = LogisticRegression(random_state=42, max_iter=1000)
            lr_model.fit(X_train, y_train)
        
        # Predictions
            y_pred = lr_model.predict(X_test)
            y_pred_proba = lr_model.predict_proba(X_test)[:, 1]
            
            # 22. ĐÁNH GIÁ MÔ HÌNH
            print("\n📊 Đánh giá mô hình:")
    
    # Classification report
            print("\n📋 Classification Report:")
    print(classification_report(y_test, y_pred))
            
            # AUC score
            auc_score = roc_auc_score(y_test, y_pred_proba)
            print(f"\n🎯 AUC Score: {auc_score:.4f}")
    
    # Confusion matrix
    cm = confusion_matrix(y_test, y_pred)
            print(f"\n📊 Confusion Matrix:")
            print(f"True Negatives: {cm[0,0]}")
            print(f"False Positives: {cm[0,1]}")
            print(f"False Negatives: {cm[1,0]}")
            print(f"True Positives: {cm[1,1]}")
            
            # 23. XUẤT DANH SÁCH USER CÓ NGUY CƠ CHURN CAO
    print("\n📋 Xuất danh sách user có nguy cơ churn cao...")
    
            # Tính churn probability cho tất cả users
            all_proba = lr_model.predict_proba(X)[:, 1]
            final_features['churn_probability'] = all_proba
            
            # Sắp xếp theo probability giảm dần
            high_risk_users = final_features[final_features['is_churned'] == 0].nlargest(100, 'churn_probability')
    
    # Lưu kết quả
            high_risk_users.to_csv(f'{output_folder}/high_churn_risk_users.csv', index=False)
            print(f"💾 Đã lưu danh sách user có nguy cơ churn cao: {output_folder}/high_churn_risk_users.csv")
            
            # Lưu model performance
            performance_summary = {
                'AUC_Score': auc_score,
                'Total_Users': len(final_features),
                'Churned_Users': final_features['is_churned'].sum(),
                'Churn_Rate': (final_features['is_churned'].sum() / len(final_features)) * 100,
                'High_Risk_Users': len(high_risk_users)
            }
            
            pd.DataFrame([performance_summary]).to_csv(f'{output_folder}/churn_model_performance.csv', index=False)
            print(f"💾 Đã lưu model performance: {output_folder}/churn_model_performance.csv")
    
    return True

# ============================================================================
# 4.3 REVENUE OPTIMIZATION
# ============================================================================
print("\n" + "="*80)
print("4.3 REVENUE OPTIMIZATION")
print("="*80)

def analyze_revenue_optimization():
    """Phân tích tối ưu doanh thu"""
    
    print("🔄 Đang đọc dữ liệu revenue...")
    
    # Đọc dữ liệu
    revenue_df = pd.read_csv('Dataset/cleaned/REVENUE.csv', low_memory=False)
    ad_revenue_df = pd.read_csv('Dataset/cleaned/AD_REVENUE.csv', low_memory=False)
    coin_gem_df = pd.read_csv('Dataset/cleaned/COIN_GEM.csv', low_memory=False)
    
    print(f"✅ Đã đọc dữ liệu revenue")
    
    # 24. PHÂN TÍCH YẾU TỐ ẢNH HƯỞNG ĐẾN NẠP TIỀN
    print("\n�� Phân tích yếu tố ảnh hưởng đến nạp tiền...")
    
    revenue_analysis = {}
    
    if 'user_pseudo_id' in revenue_df.columns:
        # Tính revenue per user
        user_revenue = revenue_df.groupby('user_pseudo_id').agg({
            'event_date': 'count'  # Giả sử mỗi dòng là 1 giao dịch
        }).rename(columns={'event_date': 'transaction_count'}).reset_index()
        
        revenue_analysis['user_revenue'] = user_revenue
        print(f"�� Tổng số giao dịch: {user_revenue['transaction_count'].sum():,}")
        print(f"�� Số user có giao dịch: {len(user_revenue):,}")
    
    # 25. SO SÁNH ARPU/ARPPU GIỮA CÁC NHÓM HÀNH VI
    print("\n�� So sánh ARPU/ARPPU giữa các nhóm hành vi...")
    
    # Tạo user segments đơn giản
    if 'user_revenue' in revenue_analysis:
        user_revenue = revenue_analysis['user_revenue']
        
        # Phân loại user theo số giao dịch
        user_revenue['user_segment'] = pd.cut(user_revenue['transaction_count'], 
                                            bins=[0, 1, 3, 10, float('inf')], 
                                            labels=['Light', 'Medium', 'Heavy', 'Whale'])
        
        segment_analysis = user_revenue.groupby('user_segment').agg({
            'transaction_count': ['count', 'mean', 'sum']
        }).round(2)
        
        print("📊 Phân tích theo user segment:")
        print(segment_analysis)
        
        # Lưu kết quả
        user_revenue.to_csv(f'{output_folder}/user_revenue_segments.csv', index=False)
        print(f"💾 Đã lưu user segments: {output_folder}/user_revenue_segments.csv")
    
    # 26. PHÂN TÍCH MỐI QUAN HỆ AD REVENUE ↔ IAP REVENUE
    print("\n�� Phân tích mối quan hệ ad revenue ↔ IAP revenue...")
    
    # So sánh quy mô dữ liệu
    print(f"📊 IAP Revenue records: {len(revenue_df):,}")
    print(f"�� Ad Revenue records: {len(ad_revenue_df):,}")
    
    # Tính tổng revenue mỗi loại
    if len(revenue_df) > 0:
        print("📊 IAP Revenue có dữ liệu")
    if len(ad_revenue_df) > 0:
        print("📊 Ad Revenue có dữ liệu")
    
    # 27. XÁC ĐỊNH THỜI ĐIỂM VÀNG
    print("\n�� Xác định thời điểm vàng...")
    
    # Phân tích theo thời gian (nếu có cột thời gian)
    if 'event_date' in revenue_df.columns:
        try:
            revenue_df['event_date'] = pd.to_datetime(revenue_df['event_date'], errors='coerce')
            revenue_df = revenue_df.dropna(subset=['event_date'])
            
            # Nhóm theo ngày
            daily_revenue = revenue_df.groupby(revenue_df['event_date'].dt.date).size()
            
            # Tìm ngày có revenue cao nhất
            peak_day = daily_revenue.idxmax()
            peak_value = daily_revenue.max()
            
            print(f"📅 Ngày có revenue cao nhất: {peak_day} ({peak_value} giao dịch)")
            
            # Vẽ biểu đồ revenue theo ngày
            plt.figure(figsize=(12, 6))
            daily_revenue.plot(kind='line', marker='o')
            plt.title('Revenue theo ngày', fontsize=14, fontweight='bold')
            plt.xlabel('Ngày', fontsize=12)
            plt.ylabel('Số giao dịch', fontsize=12)
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(f'{output_folder}/revenue_timeline.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"📊 Đã lưu biểu đồ revenue timeline: {output_folder}/revenue_timeline.png")
            
        except Exception as e:
            print(f"⚠️ Không thể phân tích theo thời gian: {e}")
    
    return revenue_analysis

# ============================================================================
# CHẠY TẤT CẢ PHÂN TÍCH
# ============================================================================
print("\n" + "="*80)
print("�� BẮT ĐẦU CHẠY TẤT CẢ PHÂN TÍCH...")
print("="*80)

try:
    # Import các thư viện cần thiết
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
    
    print("✅ Đã import đầy đủ thư viện")
    
    # Chạy phân tích
    print("\n🔄 Đang chạy phân tích hành vi người chơi...")
    user_features, cluster_analysis, retention_analysis = analyze_player_behavior()
    
    print("\n🔄 Đang chạy churn prediction...")
    churn_result = analyze_churn_prediction()
    
    print("\n🔄 Đang chạy revenue optimization...")
    revenue_result = analyze_revenue_optimization()
    
    print("\n�� Tất cả phân tích đã hoàn thành thành công!")
    
except Exception as e:
    print(f"\n❌ Có lỗi xảy ra: {e}")
    import traceback
    traceback.print_exc()

print("\n🎯 Giai đoạn 4 hoàn thành! Bạn đã có đầy đủ insights cho 3 đề tài chính.")