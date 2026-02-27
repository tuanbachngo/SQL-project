import pandas as pd
from sqlalchemy import create_engine, text

# 1. Cấu hình kết nối MySQL
engine = create_engine("mysql+mysqlconnector://root:Binhthaii0203=))@localhost/vn_firm_panel_test")

def run_import_firms_complete(excel_path):
    try:
        print(f"--- Đang đọc file: {excel_path} ---")
        all_sheets = pd.read_excel(excel_path, sheet_name=None)
        
        # Kiểm tra đủ 2 sheet quan trọng
        if 'company_info' not in all_sheets or 'source_info' not in all_sheets:
            print("Lỗi: Thiếu sheet 'company_info' hoặc 'source_info'!")
            return

        # Làm sạch dữ liệu nan -> None
        df_firms = all_sheets['company_info'].where(pd.notnull(all_sheets['company_info']), None)
        df_sources = all_sheets['source_info'].where(pd.notnull(all_sheets['source_info']), None)
        
        with engine.connect() as conn:
            # BƯỚC 1: RESET DATABASE (Để mọi ID quay về 1)
            print("Đang dọn dẹp database và reset bộ đếm ID...")
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            conn.execute(text("TRUNCATE TABLE dim_firm;"))
            conn.execute(text("TRUNCATE TABLE dim_industry_l2;"))
            conn.execute(text("TRUNCATE TABLE dim_exchange;"))
            conn.execute(text("TRUNCATE TABLE dim_data_source;")) # Thêm reset bảng source
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
            conn.commit()

            # BƯỚC 2: NẠP NGUỒN DỮ LIỆU (Từ sheet source_info)
            print("Đang nạp danh mục nguồn dữ liệu...")
            for _, s_row in df_sources.iterrows():
                conn.execute(text("""
                    INSERT INTO dim_data_source (source_name, source_type, provider, note)
                    VALUES (:name, :type, :prov, :note)
                """), {
                    "name": s_row['source_name'],
                    "type": s_row['source_type'],
                    "prov": s_row['provider'],
                    "note": s_row['note']
                })

            # BƯỚC 3: XỬ LÝ SÀN (HOSE=1, HNX=2...)
            print("Đang nạp danh mục sàn giao dịch...")
            unique_exchanges = []
            for exc_code in df_firms['exchange_code']:
                if exc_code and exc_code not in [e[0] for e in unique_exchanges]:
                    exc_name = df_firms[df_firms['exchange_code'] == exc_code]['exchange_name'].iloc[0]
                    unique_exchanges.append((exc_code, exc_name))
            
            for code, name in unique_exchanges:
                conn.execute(text("INSERT INTO dim_exchange (exchange_code, exchange_name) VALUES (:c, :n)"), 
                             {"c": code, "n": name})

            # BƯỚC 4: XỬ LÝ NGÀNH (Thẳng hàng 1 -> 8)
            print("Đang nạp danh mục ngành hàng...")
            unique_industries = []
            for ind in df_firms['industry_l2_name']:
                if ind and ind not in unique_industries:
                    unique_industries.append(ind)
            
            for ind_name in unique_industries:
                conn.execute(text("INSERT INTO dim_industry_l2 (industry_l2_name) VALUES (:n)"), {"n": ind_name})

            # BƯỚC 5: NẠP DOANH NGHIỆP
            print("Đang nạp danh sách doanh nghiệp...")
            for _, row in df_firms.iterrows():
                exc_id = conn.execute(text("SELECT exchange_id FROM dim_exchange WHERE exchange_code = :c"),
                                       {"c": row['exchange_code']}).fetchone()[0]
                
                ind_id = conn.execute(text("SELECT industry_l2_id FROM dim_industry_l2 WHERE industry_l2_name = :n"),
                                      {"n": row['industry_l2_name']}).fetchone()[0]

                conn.execute(text("""
                    INSERT INTO dim_firm (ticker, company_name, exchange_id, industry_l2_id, founded_year, listed_year, status)
                    VALUES (:ticker, :name, :ex_id, :ind_id, :f_year, :l_year, :status)
                """), {
                    "ticker": row['ticker'], "name": row['company_name'],
                    "ex_id": exc_id, "ind_id": ind_id,
                    "f_year": row['founded_year'], "l_year": row['listed_year'],
                    "status": row.get('status', 'active')
                })
            
            conn.commit()
            print("\n--- HOÀN THÀNH TẤT CẢ ---")
            print("ID của tất cả các bảng DIM đã thẳng hàng và đầy đủ dữ liệu.")

    except Exception as e:
        print(f"Lỗi khi thực hiện Script A: {e}")

if __name__ == "__main__":
    run_import_firms_complete("data/ttin cty.xlsx")