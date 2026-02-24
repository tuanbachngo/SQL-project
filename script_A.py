import pandas as pd
from sqlalchemy import create_engine, text

# Cấu hình kết nối MySQL
engine = create_engine("mysql+mysqlconnector://root:Binhthaii0203=))@localhost/vn_firm_panel_test")

def run_import_firms(excel_path):
    try:
        # 1. Đọc dữ liệu từ 2 sheet
        print(f"Đang đọc file: {excel_path}...")
        
        # Sử dụng pd.read_excel trực tiếp
        all_sheets = pd.read_excel(excel_path, sheet_name=None) # Đọc toàn bộ để kiểm tra
        
        if 'company_info' not in all_sheets or 'source_info' not in all_sheets:
            print(f"Lỗi: File Excel thiếu sheet 'company_info' hoặc 'source_info'!")
            print(f"Các sheet đang có: {list(all_sheets.keys())}")
            return

        df_firms = all_sheets['company_info']
        df_sources = all_sheets['source_info']
        
        # 2. CHỈ xử lý .where() khi chắc chắn df không phải None
        df_firms = df_firms.where(pd.notnull(df_firms), None)
        df_sources = df_sources.where(pd.notnull(df_sources), None)
        
        with engine.connect() as conn:
            # Tiếp tục phần nạp dữ liệu vào SQL...
            print("--- Kết nối database thành công ---")
            
            # BƯỚC A: Xử lý danh mục Nguồn dữ liệu (dim_data_source)
            # Yêu cầu: source_name, source_type 
            for _, s_row in df_sources.iterrows():
                conn.execute(text("""
                    INSERT INTO dim_data_source (source_name, source_type, provider, note)
                    VALUES (:name, :type, :prov, :note)
                    ON DUPLICATE KEY UPDATE
                        source_type = VALUES(source_type),
                        provider = VALUES(provider),
                        note = VALUES(note)
                """), {
                    "name": s_row['source_name'],
                    "type": s_row['source_type'],
                    "prov": s_row.get('provider'),
                    "note": s_row.get('note')
                })

            # BƯỚC B: Xử lý danh mục Doanh nghiệp và các ràng buộc
            for _, row in df_firms.iterrows():
                # 1. Cập nhật sàn giao dịch (dim_exchange) [cite: 37]
                conn.execute(text("""
                    INSERT INTO dim_exchange (exchange_code, exchange_name) 
                    VALUES (:code, :name)
                    ON DUPLICATE KEY UPDATE exchange_name = VALUES(exchange_name)
                """), {"code": row['exchange_code'], "name": row.get('exchange_name')})
                
                exc_id = conn.execute(text("SELECT exchange_id FROM dim_exchange WHERE exchange_code = :c"),
                                       {"c": row['exchange_code']}).fetchone()[0]

                # 2. Cập nhật ngành L2 (dim_industry_l2) [cite: 37]
                conn.execute(text("""
                    INSERT IGNORE INTO dim_industry_l2 (industry_l2_name) 
                    VALUES (:name)
                """), {"name": row['industry_l2_name']})
                
                ind_id = conn.execute(text("SELECT industry_l2_id FROM dim_industry_l2 WHERE industry_l2_name = :n"),
                                      {"n": row['industry_l2_name']}).fetchone()[0]

                # 3. Đổ dữ liệu vào bảng doanh nghiệp (dim_firm) [cite: 37, 48, 57]
                conn.execute(text("""
                    INSERT INTO dim_firm (ticker, company_name, exchange_id, industry_l2_id, founded_year, listed_year, status)
                    VALUES (:ticker, :name, :ex_id, :ind_id, :f_year, :l_year, :status)
                    ON DUPLICATE KEY UPDATE 
                        company_name = VALUES(company_name),
                        exchange_id = VALUES(exchange_id),
                        industry_l2_id = VALUES(industry_l2_id),
                        founded_year = VALUES(founded_year),
                        listed_year = VALUES(listed_year),
                        status = VALUES(status)
                """), {
                    "ticker": row['ticker'],
                    "name": row['company_name'],
                    "ex_id": exc_id,
                    "ind_id": ind_id,
                    "f_year": row.get('founded_year'),
                    "l_year": row.get('listed_year'),
                    "status": row.get('status', 'active')
                })
            
            conn.commit()
            print("--- Hoàn thành đổ dữ liệu vào 4 bảng DIM! ---")

    except Exception as e:
        print(f"Lỗi khi thực hiện Script A: {e}")

if __name__ == "__main__":
    # Đảm bảo file Excel của bạn có 2 sheet tên là 'company_info' và 'source_info'
    run_import_firms("data/ttin cty.xlsx")