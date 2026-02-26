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

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

def get_financial_data():
    """Lấy dữ liệu tài chính tổng hợp từ View"""
    try:
        conn = get_db_connection()
        query = "SELECT * FROM vw_firm_panel_latest"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu từ View: {e}")
        return None

def get_market_data():
    """Lấy giá cổ phiếu trực tiếp từ bảng fact_market_year (Snapshot mới nhất)"""
    try:
        conn = get_db_connection()
        query = """
            SELECT f.ticker, m.fiscal_year, m.share_price 
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY firm_id, fiscal_year ORDER BY snapshot_id DESC) as rn
                FROM fact_market_year
            ) m
            JOIN dim_firm f ON m.firm_id = f.firm_id
            WHERE m.rn = 1
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu Market: {e}")
        return pd.DataFrame(columns=['ticker', 'fiscal_year', 'share_price'])

def get_innovation_data():
    """Lấy dữ liệu đổi mới và note trực tiếp từ bảng fact_innovation_year"""
    try:
        conn = get_db_connection()
        query = """
            SELECT f.ticker, iv.fiscal_year, iv.evidence_note 
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY firm_id, fiscal_year ORDER BY snapshot_id DESC) as rn
                FROM fact_innovation_year
            ) iv
            JOIN dim_firm f ON iv.firm_id = f.firm_id
            WHERE iv.rn = 1
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu Innovation: {e}")
        return pd.DataFrame()

def run_qc_checks(df, df_mkt, df_inn):
    """Thực hiện các quy định QC tối thiểu theo yêu cầu dự án"""
    qc_results = []
    df = pd.merge(df, df_mkt, on=['ticker', 'fiscal_year'], how='left')
    df = pd.merge(df, df_inn, on=['ticker', 'fiscal_year'], how='left')

    for index, row in df.iterrows():
        ticker = row['ticker']
        year = row['fiscal_year']

        # Các điều kiện kiểm tra tối thiểu
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

        # 3. Kiểm tra các trường không âm
        non_negative_fields = [  
            'total_sales_revenue',           
            'net_sales',              
            'total_assets',                       
            'intangible_assets_net',                       
            'total_liabilities',            
            'cash_and_equivalents',       
            'long_term_debt',                 
            'current_assets',                  
            'current_liabilities',           
            'inventory',                
            'dividend_cash_paid',               
            'net_ppe',
            'wip_goods_purchase',
            'merchandise_purchase_year',
            'firm_age',
            'employees_count'
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
            'manufacturing_overhead',         
            'raw_material_consumption',                            
            'outside_manufacturing_expenses', 
            'production_cost',                
            'rnd_expenses', 
            'capex',  
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
        
        # Các điều kiện thêm ngoài 6 mục tối thiểu
        # 7. Kiểm tra tính cân đối của bảng cân đối kế toán
        assets = row.get('total_assets')
        liabilities = row.get('total_liabilities')
        equity = row.get('total_equity')

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

       # 8. Kiểm tra tính hợp lý thành phần tài sản
        curr_assets = row.get('current_assets')
        if assets is not None and curr_assets is not None and curr_assets > assets:
            qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 'field_name': 'current_assets',
                'error_type': 'COMPONENT_ERROR', 'message': 'Tài sản ngắn hạn vượt quá tổng tài sản'
            })

        cash = row.get('cash_and_equivalents')
        inv = row.get('total_inventory')
        if curr_assets is not None and cash is not None and inv is not None and (cash + inv) > curr_assets:
            qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 'field_name': 'cash_and_equivalents/total_inventory',
                'error_type': 'COMPONENT_ERROR', 'message': 'Tổng tiền mặt và hàng tồn kho vượt quá tài sản ngắn hạn'
            })

        ppe = row.get('net_ppe')
        intang = row.get('intangible_assets_net')
        if assets is not None and ppe is not None and intang is not None and (ppe + intang) > assets:
            qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 'field_name': 'net_ppe/intangible_assets_net',
                'error_type': 'COMPONENT_ERROR', 'message': 'Tổng PPE và tài sản vô hình vượt quá tổng tài sản'
            })

        # 9. Kiểm tra tính hợp lý thành phần nợ
        lt_debt = row.get('long_term_debt')
        curr_liab = row.get('current_liabilities')
        if liabilities is not None and lt_debt is not None and curr_liab is not None and (lt_debt + curr_liab) > liabilities:
                qc_results.append({
                    'ticker': ticker, 'fiscal_year': year, 'field_name': 'long_term_debt',
                    'error_type': 'COMPONENT_ERROR', 'message': 'Nợ dài hạn và nợ ngắn hạn vượt quá tổng nợ'
                })

        # 10. Kiểm tra các biến về doanh thu và lợi nhuận:
        revenue = row.get('net_sales')
        net_income = row.get('net_income')
        if revenue is not None and net_income is not None and net_income > revenue:
            qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 'field_name': 'net_income',
                'error_type': 'COMPONENT_ERROR', 'message': 'Lợi nhuận ròng vượt quá doanh thu'
            })

        total_revenue = row.get('total_sales_revenue')
        if total_revenue is not None and revenue is not None and revenue > total_revenue:
            qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 'field_name': 'net_sales',
                'error_type': 'COMPONENT_ERROR', 'message': 'Doanh thu thuần vượt quá doanh thu tổng'
            })

        # 11. Kiểm tra các trường dummy (0/1)
        prod = row['product_innovation']
        proc = row['process_innovation']
        note = str(row['evidence_note']).strip() if row['evidence_note'] else ""

        # Kiểm tra giá trị dummy chỉ được là 0 hoặc 1
        for field in ['product_innovation', 'process_innovation']:
            val = row.get(field)
            if val is not None and val not in [0, 1]:
                qc_results.append({
                    'ticker': ticker, 'fiscal_year': year, 'field_name': field,
                    'error_type': 'INVALID_DUMMY', 'message': f'Giá trị {field} phải là 0 hoặc 1'
                })

        # Check Dummy - Note relationship
        if (prod == 1 or proc == 1) and (not note or note.lower() in ['nan', 'none']):
            qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 'field_name': 'evidence_note',
                'error_type': 'MISSING_NOTE', 'message': 'Đã đánh dấu Đổi mới (1) nhưng thiếu thuyết minh (Note)'
            })
        
        if (prod == 0 and proc == 0) and (note and note.lower() not in ['nan', 'none']):
             qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 'field_name': 'evidence_note',
                'error_type': 'UNEXPECTED_NOTE', 'message': 'Có note thuyết minh nhưng biến Dummy đều là 0'
            })

    return pd.DataFrame(qc_results)

def main():
    if not os.path.exists('outputs'):
        os.makedirs('outputs')

    print("Đang tải dữ liệu từ Database...")
    df_fin = get_financial_data()
    df_mkt = get_market_data()
    df_inn = get_innovation_data()
    
    if df_fin is not None:
        print("Đang chạy kiểm tra QC...")
        report_df = run_qc_checks(df_fin, df_mkt, df_inn)
        
        if not report_df.empty:
            report_path = 'outputs/qc_report_final.csv'
            report_df.to_csv(report_path, index=False, encoding='utf-8-sig')
            print(f"Đã tìm thấy {len(report_df)} cảnh báo. Chi tiết tại: {report_path}")
        else:
            print("Chúc mừng! Dữ liệu không có lỗi logic nào.")
    else:
        print("Không thể kết nối để lấy dữ liệu.")

if __name__ == "__main__":
    main()


