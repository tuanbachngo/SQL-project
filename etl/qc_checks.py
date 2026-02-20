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

def run_qc_checks(df, df_ext):
    """Thực hiện các quy định QC tối thiểu theo yêu cầu dự án"""
    qc_results = []
    df = pd.merge(
        df, 
        df_ext[['ticker', 'fiscal_year', 'share_price']], 
        on=['ticker', 'fiscal_year'], 
        how='left', 
        suffixes=('', '_ext')
    )

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
        shares = row.get('total_share_outstanding')
        if shares is not None and shares <= 0:
            qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 'field_name': 'total_share_outstanding',
                'error_type': 'INVALID_VALUE', 'message': 'Số lượng cổ phiếu phải lớn hơn 0'
            })

        # 3. Kiểm tra các trường không âm
        non_negative_fields = [  
            'total_sales_revenue',           
            'net_sales_revenue',              
            'total_assets',                       
            'intangible_assets_value',                       
            'total_liabilities',            
            'cash_and_cash_equivalent',       
            'long_term_debt',                 
            'current_assets',                  
            'current_liabilities',           
            'total_inventory',                
            'dividend_payment',               
            'net_ppe'
        ]
        for field in non_negative_fields:
            val = row.get(field)
            if val is not None and val < 0:
                qc_results.append({
                    'ticker': ticker, 'fiscal_year': year, 'field_name': field,
                    'error_type': 'NEGATIVE_VALUE', 'message': f'Giá trị {field} không được âm'
                })

        # 4. Kiểm tra các trường không dương (chi phí)
        non_positive_fields = [
            'selling_expenses',             
            'general_admin_expenses',
            'overhead_manufacturing',         
            'raw_material_consumption',       
            'merchandise_purchase',          
            'wip_goods_purchase',            
            'outside_manufacturing_expenses', 
            'production_cost',                
            'rnd_expenditure', 
            'capital_expenditure',  
        ]
        for field in non_positive_fields:
            val = row.get(field)
            if val is not None and val > 0:
                qc_results.append({
                    'ticker': ticker, 'fiscal_year': year, 'field_name': field,
                    'error_type': 'POSITIVE_VALUE', 'message': f'Giá trị {field} không được dương'
                })

        # 5. Kiểm tra Growth ratio 
        growth = row.get('growth_ratio')
        if growth is not None:
            if growth < GROWTH_LIMITS[0] or growth > GROWTH_LIMITS[1]:
                qc_results.append({
                    'ticker': ticker, 'fiscal_year': year, 'field_name': 'growth_ratio',
                    'error_type': 'OUTLIER', 'message': f'Tỷ lệ tăng trưởng {growth} bất thường'
                })

        # 6. Kiểm tra tính nhất quán Market Cap
        price = row.get('share_price_ext') if pd.notnull(row.get('share_price_ext')) else row.get('share_price')
        mkt_cap = row.get('market_value_equity')
        if all(v is not None for v in [shares, price, mkt_cap]):
            calculated_cap = shares * price
            diff = abs(calculated_cap - mkt_cap) / mkt_cap if mkt_cap != 0 else 0
            if diff > MARKET_CAP_TOLERANCE:
                qc_results.append({
                    'ticker': ticker, 'fiscal_year': year, 'field_name': 'market_value_equity',
                    'error_type': 'INCONSISTENT', 'message': f'Sai lệch vốn hóa ({diff:.2%}) so với tính toán'
                })
        
        # 7. Kiểm tra tính cân đối của bảng cân đối kế toán
        assets = row.get('total_assets')
        liabilities = row.get('total_liabilities')
        equity = row.get('total_shareholders_equity')

        # Kiểm tra nếu cả 3 biến đều có dữ liệu
        if all(v is not None for v in [assets, liabilities, equity]):
            # Công thức: Tài sản = Nợ phải trả + Vốn chủ sở hữu
            total_source = liabilities + equity
            absolute_diff = abs(assets - total_source)
            
            # Tính tỷ lệ sai lệch tương đối
            relative_diff = absolute_diff / assets if assets != 0 else 0

            if relative_diff > MARKET_CAP_TOLERANCE:
                qc_results.append({
                    'ticker': ticker,
                    'fiscal_year': year,
                    'field_name': 'total_assets/liabilities/equity',
                    'error_type': 'ACCOUNTING_IMBALANCE',
                    'message': f'Bảng cân đối không khớp. Chênh lệch: {absolute_diff:,.0f} ({relative_diff:.2%})'
                })

       # 8. Kiểm tra tính hợp lý của các khoản mục cấu thành
        curr_assets = row.get('current_assets')
        if assets is not None and curr_assets is not None and curr_assets > assets:
            qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 'field_name': 'current_assets',
                'error_type': 'COMPONENT_ERROR', 'message': 'Tài sản ngắn hạn vượt quá tổng tài sản'
            })

        cash = row.get('cash_and_cash_equivalents')
        inv = row.get('total_inventory')
        if curr_assets is not None:
            if cash is not None and cash > curr_assets:
                qc_results.append({
                    'ticker': ticker, 'fiscal_year': year, 'field_name': 'cash',
                    'error_type': 'COMPONENT_ERROR', 'message': 'Tiền mặt vượt quá tài sản ngắn hạn'
                })
            if inv is not None and inv > curr_assets:
                qc_results.append({
                    'ticker': ticker, 'fiscal_year': year, 'field_name': 'inventory',
                    'error_type': 'COMPONENT_ERROR', 'message': 'Hàng tồn kho vượt quá tài sản ngắn hạn'
                })

        lt_debt = row.get('long_term_debt')
        curr_liab = row.get('current_liabilities')
        if liabilities is not None:
            if lt_debt is not None and lt_debt > liabilities:
                qc_results.append({
                    'ticker': ticker, 'fiscal_year': year, 'field_name': 'long_term_debt',
                    'error_type': 'COMPONENT_ERROR', 'message': 'Nợ dài hạn vượt quá tổng nợ'
                })
            if curr_liab is not None and curr_liab > liabilities:
                qc_results.append({
                    'ticker': ticker, 'fiscal_year': year, 'field_name': 'current_liabilities',
                    'error_type': 'COMPONENT_ERROR', 'message': 'Nợ ngắn hạn vượt quá tổng nợ'
                })

        # 9. Kiểm tra các trường dummy (0/1)
        for field in ['product_innovation', 'process_innovation']:
            val = row.get(field)
            if val is not None and val not in [0, 1]:
                qc_results.append({
                    'ticker': ticker, 'fiscal_year': year, 'field_name': field,
                    'error_type': 'INVALID_DUMMY', 'message': f'Trường {field} phải là 0 hoặc 1'
                })

    return pd.DataFrame(qc_results)

def main():
    if not os.path.exists('outputs'):
        os.makedirs('outputs')

    df_db = get_latest_data()
    
    price_csv_path = "data/external_share_prices.csv"
    if os.path.exists(price_csv_path):
        df_ext = pd.read_csv(price_csv_path)
    else:
        df_ext = pd.DataFrame(columns=['ticker', 'fiscal_year', 'share_price'])
    
    if df_db is not None and not df_db.empty:
        report_df = run_qc_checks(df_db, df_ext)
        report_path = 'outputs/qc_report.csv'
        report_df.to_csv(report_path, index=False, encoding='utf-8-sig')

        print(f"Hoàn thành! Báo cáo đã được lưu tại: {report_path}")
        print(f"Tổng số lỗi phát hiện: {len(report_df)}")
    else:
        print("Không có dữ liệu để kiểm tra.")
        
def test_qc_logic(): # Tạo dữ liệu giả lập 
    data = {
        'ticker': ['GVR', 'GVR'],
        'fiscal_year': [2023, 2024],
        'total_assets': [1000, -500], # Dòng 2 cố tình để âm để test
        'total_liabilities': [600, 300],
        'total_shareholders_equity': [400, 100], # Dòng 1 cân (600+400=1000), Dòng 2 lệch
        'current_assets': [500, 200],
        'cash_and_cash_equivalent': [600, 50], # Dòng 1 lỗi: Tiền > Tài sản ngắn hạn
        'total_share_outstanding': [100, 100],
        'market_value_equity': [2000, 3000]
    }
    df_test = pd.DataFrame(data)
    
    # Giả lập file external price
    df_ext = pd.DataFrame({
        'ticker': ['GVR', 'GVR'],
        'fiscal_year': [2023, 2024],
        'share_price': [20, 25]
    })
    
    # Chạy thử hàm QC
    report = run_qc_checks(df_test, df_ext)
    print(report)

if __name__ == "__main__":
    test_qc_logic() # chạy thử dữ liệu giả lập
