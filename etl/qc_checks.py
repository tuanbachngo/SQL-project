from sqlalchemy import text
import pandas as pd
import os
from database_setup import engine
#====================================================================
GROWTH_LIMITS = (-0.95, 5.0)
MARKET_CAP_TOLERANCE = 0.05  # Cho phép sai số 5%
#====================================================================
def find_table(engine, column_name):
    query = text("""
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE COLUMN_NAME = :col 
          AND TABLE_SCHEMA = 'vn_firm_panel_test'
          AND TABLE_NAME LIKE 'fact_%'
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"col": column_name}).fetchone()
        return result[0] if result else None
#====================================================================   
def find_tables_for_columns(engine, columns):
    tables = set() 
    
    for col in columns:
        tbl = find_table(engine, col)
        if tbl:
            tables.add(tbl)
            
    return ", ".join(sorted(list(tables))) if tables else "Unknown"
#====================================================================    
def get_data():
    """Lấy dữ liệu từ View và Join với các bảng để lấy thêm dữ liệu cần thiết cho QC checks"""
    try:
        query = """
            SELECT 
                v.*, 
                f.founded_year,
                m.share_price,
                i.evidence_note
            FROM vw_firm_panel_latest v
            -- Lấy năm thành lập từ bảng Dim
            LEFT JOIN dim_firm f ON v.firm_id = f.firm_id
            -- Lấy giá mới nhất từ bảng Fact Market bằng Subquery
            LEFT JOIN (
                SELECT firm_id, fiscal_year, share_price
                FROM (
                    SELECT firm_id, fiscal_year, share_price, 
                        ROW_NUMBER() OVER (PARTITION BY firm_id, fiscal_year ORDER BY snapshot_id DESC) as rn
                    FROM fact_market_year
                ) t1 WHERE rn = 1
            ) m ON v.firm_id = m.firm_id AND v.fiscal_year = m.fiscal_year
            -- Lấy Note mới nhất từ bảng Fact Innovation
            LEFT JOIN (
                SELECT firm_id, fiscal_year, evidence_note
                FROM (
                    SELECT firm_id, fiscal_year, evidence_note, 
                        ROW_NUMBER() OVER (PARTITION BY firm_id, fiscal_year ORDER BY snapshot_id DESC) as rn
                    FROM fact_innovation_year
                ) t2 WHERE rn = 1
            ) i ON v.firm_id = i.firm_id AND v.fiscal_year = i.fiscal_year
            """
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu tổng hợp: {e}")
        return None
#====================================================================   
def run_qc_checks(df):
    """Thực hiện các quy định QC"""
    qc_results = []
    df = df.sort_values(by=['ticker', 'fiscal_year']) 

    for ticker, group in df.groupby('ticker'):
        # Chuyển nhóm về list để duyệt theo chỉ mục (index)
        rows = group.to_dict('records')
        
        for i in range(len(rows)):
            row = rows[i]
            year = row['fiscal_year']
            age = row.get('firm_age')
            founded = row.get('founded_year')

            # 1. Kiểm tra logic tuổi doanh nghiệp (firm_age) dựa trên năm thành lập (founded_year)
            if founded is not None and age is not None:
                expected_age = year - founded if year >= founded else None 
                if age != expected_age:
                    qc_results.append({
                        'ticker': ticker, 
                        'fiscal_year': year, 
                        'table_name': find_table(engine, 'firm_age'),
                        'column_name': 'firm_age',
                        'error_type': 'AGE_LOGIC_ERROR', 
                        'message': f"Không khớp năm thành lập. Có: {age}, Tính toán: {expected_age}",       
                        'old_value': age,
                    })

            # 2. Kiểm tra tính tiến triển của tuổi doanh nghiệp qua các năm (firm_age phải tăng dần theo năm)
            if i > 0:
                prev_row = rows[i-1]
                # Check nếu năm tăng 1 thì tuổi phải tăng 1
                if year == prev_row['fiscal_year'] + 1:
                    if age is not None and prev_row['firm_age'] is not None:
                        if age != prev_row['firm_age'] + 1:
                            qc_results.append({
                                'ticker': ticker, 
                                'fiscal_year': year, 
                                'table_name': find_table(engine, 'firm_age'), 
                                'column_name': 'firm_age',
                                'error_type': 'PROGRESSION_ERROR', 
                                'message': f"Tuổi không tăng tiến đều (Năm trước: {prev_row['firm_age']}, Năm nay: {age})",
                                'old_value': age,
                            })
                # Check nếu bị mất năm (Time Gap)
                elif year > prev_row['fiscal_year'] + 1:
                    qc_results.append({
                        'ticker': ticker, 
                        'fiscal_year': f"{prev_row['fiscal_year']}-{year}",
                        'table_name': None, 
                        'column_name': 'fiscal_year', 
                        'error_type': 'TIME_GAP', 
                        'message': "Dữ liệu bị đứt quãng thời gian",
                        'old_value': None
                    })

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
                    'ticker': ticker, 
                    'fiscal_year': year, 
                    'table_name': find_table(engine, field),
                    'column_name': field,
                    'error_type': 'RANGE_ERROR', 
                    'message': f'Giá trị {val} nằm ngoài khoảng [0,1]',
                    'old_value': val
                })

        # 2. Kiểm tra Shares outstanding > 0
        shares = row.get('shares_outstanding')
        if shares is not None and shares <= 0:
            qc_results.append({
                'ticker': ticker, 
                'fiscal_year': year, 
                'table_name': find_table(engine, 'shares_outstanding'),
                'column_name': 'shares_outstanding',
                'error_type': 'INVALID_VALUE', 
                'message': 'Số lượng cổ phiếu phải lớn hơn 0',
                'old_value': shares
            })

        # 3. Kiểm tra các trường không âm
        non_negative_fields = [            
            'net_sales',              
            'total_assets',                       
            'intangible_assets_net',                       
            'total_liabilities',            
            'cash_and_equivalents',       
            'long_term_debt',                 
            'current_assets',                  
            'current_liabilities',           
            'inventory',                               
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
                    'ticker': ticker, 
                    'fiscal_year': year, 
                    'table_name': find_table(engine, field),
                    'column_name': field,
                    'error_type': 'NEGATIVE_VALUE', 
                    'message': f'Giá trị {field} không được âm',
                    'old_value': val
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
            'dividend_cash_paid'
        ]
        for field in non_positive_fields:
            val = row.get(field)
            if val is not None and val > 0:
                qc_results.append({
                    'ticker': ticker, 
                    'fiscal_year': year, 
                    'table_name': find_table(engine, field),
                    'column_name': field,
                    'error_type': 'POSITIVE_VALUE', 
                    'message': f'Giá trị {field} không được dương',
                    'old_value': val
                })

        # 5. Kiểm tra Growth ratio 
        growth = row.get('growth_ratio')
        if growth is not None:
            if growth < GROWTH_LIMITS[0] or growth > GROWTH_LIMITS[1]:
                qc_results.append({
                    'ticker': ticker, 
                    'fiscal_year': year, 
                    'table_name': find_table(engine, 'growth_ratio'), 
                    'column_name': 'growth_ratio',
                    'error_type': 'OUTLIER', 
                    'message': f'Tỷ lệ tăng trưởng {growth} bất thường',
                    'old_value': growth
                })

        # 6. Kiểm tra tính nhất quán Market Cap
        price = row.get('share_price')
        mkt_cap = row.get('market_value_equity')
        if all(v is not None for v in [shares, price, mkt_cap]):
            calculated_cap = shares * price
            diff = abs(calculated_cap - mkt_cap) / mkt_cap if mkt_cap != 0 else 0
            if diff > MARKET_CAP_TOLERANCE:
                qc_results.append({
                    'ticker': ticker, 
                    'fiscal_year': year, 
                    'table_name': find_tables_for_columns(engine, ['market_value_equity', 'shares_outstanding', 'share_price']),
                    'column_name': 'market_value_equity/shares_outstanding/share_price',
                    'error_type': 'INCONSISTENT', 
                    'message': f'Sai lệch vốn hóa ({diff:.2%}) so với tính toán',
                    'old_value': (mkt_cap, shares, price)
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
                    'table_name': find_tables_for_columns(engine, ['total_assets', 'total_liabilities', 'total_equity']),
                    'column_name': 'total_assets/total_liabilities/total_equity',
                    'error_type': 'ACCOUNTING_IMBALANCE',
                    'message': f'Bảng cân đối không khớp. Chênh lệch: {absolute_diff:,.0f} ({relative_diff:.2%})',
                    'old_value': (assets, liabilities, equity)
                })

       # 8. Kiểm tra tính hợp lý thành phần tài sản
        curr_assets = row.get('current_assets')
        if assets is not None and curr_assets is not None and curr_assets > assets:
            qc_results.append({
                'ticker': ticker, 
                'fiscal_year': year, 
                'table_name': find_tables_for_columns(engine, ['current_assets', 'total_assets']),
                'column_name': 'current_assets/total_assets',
                'error_type': 'COMPONENT_ERROR', 
                'message': 'Tài sản ngắn hạn vượt quá tổng tài sản',
                'old_value': (curr_assets, assets)
            })

        cash = row.get('cash_and_equivalents')
        inv = row.get('inventory')
        if curr_assets is not None and cash is not None and inv is not None and (cash + inv) > curr_assets:
            qc_results.append({
                'ticker': ticker, 
                'fiscal_year': year, 
                'table_name': find_tables_for_columns(engine, ['cash_and_equivalents', 'inventory', 'current_assets']),
                'column_name': 'cash_and_equivalents/inventory/current_assets',
                'error_type': 'COMPONENT_ERROR', 
                'message': 'Tổng tiền mặt và hàng tồn kho vượt quá tài sản ngắn hạn',
                'old_value': (cash, inv, curr_assets)
            })

        ppe = row.get('net_ppe')
        intang = row.get('intangible_assets_net')
        if assets is not None and ppe is not None and intang is not None and (ppe + intang) > assets:
            qc_results.append({
                'ticker': ticker, 
                'fiscal_year': year, 
                'table_name': find_tables_for_columns(engine, ['net_ppe', 'intangible_assets_net', 'total_assets']),
                'column_name': 'net_ppe/intangible_assets_net/total_assets',
                'error_type': 'COMPONENT_ERROR', 
                'message': 'Tổng PPE và tài sản vô hình vượt quá tổng tài sản',
                'old_value': (ppe, intang, assets)
            })

        # 9. Kiểm tra tính hợp lý thành phần nợ
        lt_debt = row.get('long_term_debt')
        curr_liab = row.get('current_liabilities')
        if liabilities is not None and lt_debt is not None and curr_liab is not None and (lt_debt + curr_liab) > liabilities:
                qc_results.append({
                    'ticker': ticker, 
                    'fiscal_year': year, 
                    'table_name': find_tables_for_columns(engine, ['long_term_debt', 'current_liabilities', 'total_liabilities']),
                    'column_name': 'long_term_debt/current_liabilities/total_liabilities',
                    'error_type': 'COMPONENT_ERROR', 
                    'message': 'Nợ dài hạn và nợ ngắn hạn vượt quá tổng nợ',
                    'old_value': (lt_debt, curr_liab, liabilities)
                })

        # 10. Kiểm tra các biến về doanh thu và lợi nhuận:
        revenue = row.get('net_sales')
        net_income = row.get('net_income')
        if revenue is not None and net_income is not None and net_income > revenue:
            qc_results.append({
                'ticker': ticker, 
                'fiscal_year': year, 
                'table_name': find_tables_for_columns(engine, ['net_income', 'net_sales']),
                'column_name': 'net_income/net_sales',
                'error_type': 'COMPONENT_ERROR', 
                'message': 'Lợi nhuận ròng vượt quá doanh thu',
                'old_value': (net_income, revenue)
            })

        # 11. Kiểm tra các trường dummy (0/1)
        prod = row['product_innovation']
        proc = row['process_innovation']
        note = str(row['evidence_note']).strip() if row['evidence_note'] else ""

        for field in ['product_innovation', 'process_innovation']:
            val = row.get(field)
            if pd.isna(val) or val is None:
                qc_results.append({
                    'ticker': ticker, 
                    'fiscal_year': year,
                    'table_name': find_table(engine, field),
                    'column_name': field,
                    'error_type': 'MISSING_VALUE', 
                    'message': f'Biến {field} đang bị để trống (NULL). Chỉ chấp nhận 0 hoặc 1.',
                    'old_value': None
                    })
                continue 
            
            if val not in [0, 1]:
                qc_results.append({
                    'ticker': ticker, 
                    'fiscal_year': year,
                    'table_name': find_table(engine, field),
                    'column_name': field,
                    'error_type': 'INVALID_DUMMY', 
                    'message': f'Giá trị {field} không hợp lệ (hiện tại là {val}). Chỉ chấp nhận 0 hoặc 1.',
                    'old_value': val
                })

        note_lower = note.lower()
        parts = note_lower.split('|')
        
        prod_content = ""
        proc_content = ""

        for p in parts:
            if 'product:' in p:
                prod_content = p.replace('product:', '').replace('nan', '').replace('none', '').strip()
            if 'process:' in p:
                proc_content = p.replace('process:', '').replace('nan', '').replace('none', '').strip()

        is_prod_note_empty = not prod_content
        is_proc_note_empty = not proc_content

        if prod == 1 and is_prod_note_empty:
            qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 
                'table_name': find_table(engine, 'evidence_note'),
                'column_name': 'evidence_note', 
                'error_type': 'MISSING_PRODUCT_NOTE',
                'message': f'Đổi mới SP (1) nhưng ghi chú "product:" trống hoặc nan. (Gốc: "{note}")',
                'old_value': note
            })

        if prod == 0 and not is_prod_note_empty:
            qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 
                'table_name': find_table(engine, 'evidence_note'),
                'column_name': 'evidence_note', 'error_type': 'UNEXPECTED_PRODUCT_NOTE',
                'message': f'Không đổi mới SP (0) nhưng "product:" lại có thuyết minh. (Gốc: "{note}")',
                'old_value': note
            })

        if proc == 1 and is_proc_note_empty:
            qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 
                'table_name': find_table(engine, 'evidence_note'),
                'column_name': 'evidence_note', 'error_type': 'MISSING_PROCESS_NOTE',
                'message': f'Đổi mới QT (1) nhưng ghi chú "process:" trống hoặc nan. (Gốc: "{note}")',
                'old_value': note
            })
        if proc == 0 and not is_proc_note_empty:
            qc_results.append({
                'ticker': ticker, 'fiscal_year': year, 
                'table_name': find_table(engine, 'evidence_note'),
                'column_name': 'evidence_note', 'error_type': 'UNEXPECTED_PROCESS_NOTE',
                'message': f'Không đổi mới QT (0) nhưng "process:" lại có thuyết minh. (Gốc: "{note}")',
                'old_value': note
            })

    qc_results.append({
        'new_value': None
    })  
    return pd.DataFrame(qc_results)
#====================================================================
def main():
    print("Đang tải dữ liệu từ Database...")
    df = get_data()
    
    if df is not None:
        print("Đang chạy kiểm tra QC...")
        report_df = run_qc_checks(df)
        
        if not report_df.empty and len(report_df) > 2:
            output_dir = "outputs"
            os.makedirs(output_dir, exist_ok=True)
            
            report_path = os.path.join(output_dir, "qc_report.csv")
            report_df.to_csv(report_path, index=False, encoding="utf-8-sig")
            
            print(f"Đã tìm thấy {len(report_df)-1} cảnh báo. Chi tiết tại: {report_path}")
        else:
            print("Chúc mừng! Dữ liệu không có lỗi logic nào.")
    else:
        print("Không thể kết nối để lấy dữ liệu.")

if __name__ == "__main__":
    main()
#====================================================================

