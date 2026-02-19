import mysql.connector
import pandas as pd
import os

# Cấu hình kết nối và ngưỡng kiểm tra
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'your_password',
    'database': 'vn_firm_panel'
}

GROWTH_LIMITS = (-0.95, 5.0)
MARKET_CAP_TOLERANCE = 0.05  # Cho phép sai số 5%

def get_latest_data():
    """Lấy dữ liệu từ view vw_firm_panel_latest"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        query = "SELECT * FROM vw_firm_panel_latest"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Lỗi kết nối CSDL: {e}")
        return None

def run_qc_checks(df):
    """Thực hiện các quy định QC tối thiểu theo yêu cầu dự án"""
    qc_results = []

    for index, row in df.iterrows():
        ticker = row['ticker']
        year = row['fiscal_year']

        # 1. Kiểm tra Ownership ratios trong khoảng [0, 1]
        own_fields = ['managerial_inside_own', 'state_own', 'institutional_own', 'foreign_own']
        for field in own_fields:
            val = row.get(field)
            if val is not None and (val < 0 or val > 1):
                qc_results.append({
                    'ticker': ticker, 'fiscal_year': year, 'field_name': field,
                    'error_type': 'RANGE_ERROR', 'message': f'Giá trị {val} nằm ngoài khoảng [0,1]'
                })

        # 2. Kiểm tra Shares outstanding > 0
        shares = row.get('shares_outstanding')
        if shares is not None and shares <= 0:
            qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 'field_name': 'shares_outstanding',
                'error_type': 'INVALID_VALUE', 'message': 'Số lượng cổ phiếu phải lớn hơn 0'
            })

        # 3. Kiểm tra Total assets >= 0
        assets = row.get('total_assets')
        if assets is not None and assets < 0:
            qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 'field_name': 'total_assets',
                'error_type': 'NEGATIVE_VALUE', 'message': 'Tổng tài sản không được âm'
            })

        # 4. Kiểm tra Current liabilities >= 0
        liabilities = row.get('current_liabilities')
        if liabilities is not None and liabilities < 0:
            qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 'field_name': 'current_liabilities',
                'error_type': 'NEGATIVE_VALUE', 'message': 'Nợ ngắn hạn không được âm'
            })

        # 5. Kiểm tra Growth ratio (cấu hình được)
        growth = row.get('growth_ratio')
        if growth is not None:
            if growth < GROWTH_LIMITS[0] or growth > GROWTH_LIMITS[1]:
                qc_results.append({
                    'ticker': ticker, 'fiscal_year': year, 'field_name': 'growth_ratio',
                    'error_type': 'OUTLIER', 'message': f'Tỷ lệ tăng trưởng {growth} bất thường'
                })

        # 6. Kiểm tra tính nhất quán Market Cap
        price = row.get('share_price')
        mkt_cap = row.get('market_value_equity')
        if all(v is not None for v in [shares, price, mkt_cap]):
            calculated_cap = shares * price
            diff = abs(calculated_cap - mkt_cap) / mkt_cap if mkt_cap != 0 else 0
            if diff > MARKET_CAP_TOLERANCE:
                qc_results.append({
                    'ticker': ticker, 'fiscal_year': year, 'field_name': 'market_value_equity',
                    'error_type': 'INCONSISTENT', 'message': f'Sai lệch vốn hóa ({diff:.2%}) so với tính toán'
                })

    return pd.DataFrame(qc_results)

def main():
    # 1. Tạo thư mục output nếu chưa có
    if not os.path.exists('outputs'):
        os.makedirs('outputs')

    # 2. Lấy dữ liệu
    print("Đang tải dữ liệu để kiểm tra...")
    df = get_latest_data()
    
    if df is not None and not df.empty:
        # 3. Chạy QC
        print("Đang thực hiện kiểm tra chất lượng (QC)...")
        report_df = run_qc_checks(df)
        
        # 4. Xuất file báo cáo
        report_path = 'outputs/qc_report.csv'
        report_df.to_csv(report_path, index=False, encoding='utf-8-sig')
        print(f"Hoàn thành! Báo cáo đã được lưu tại: {report_path}")
        print(f"Tổng số lỗi phát hiện: {len(report_df)}")
    else:
        print("Không có dữ liệu để kiểm tra.")

if __name__ == "__main__":
    main()
