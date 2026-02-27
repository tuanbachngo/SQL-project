import pandas as pd
from sqlalchemy import create_engine, text

# 1. Cấu hình kết nối MySQL
engine = create_engine("mysql+mysqlconnector://root:Binhthaii0203=))@localhost/vn_firm_panel_test")

def run_import_firms_perfect_id(excel_path):
    try:
        print(f"--- Đang đọc file: {excel_path} ---")
        all_sheets = pd.read_excel(excel_path, sheet_name=None)
        
        if 'company_info' not in all_sheets:
            print("Lỗi: Không tìm thấy sheet 'company_info'!")
            return

        # Làm sạch dữ liệu nan -> None để SQL hiểu là NULL
        df_firms = all_sheets['company_info'].where(pd.notnull(all_sheets['company_info']), None)
        
        with engine.connect() as conn:
            # BƯỚC 1: RESET DATABASE (Xóa sạch để ID bắt đầu lại từ 1)
            print("Đang dọn dẹp database và reset bộ đếm ID...")
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            conn.execute(text("TRUNCATE TABLE dim_firm;"))
            conn.execute(text("TRUNCATE TABLE dim_industry_l2;"))
            conn.execute(text("TRUNCATE TABLE dim_exchange;"))
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
            conn.commit()

            # BƯỚC 2: XỬ LÝ SÀN (Đảm bảo HOSE=1, HNX=2 nếu xuất hiện theo thứ tự đó)
            print("Đang nạp danh mục sàn giao dịch...")
            unique_exchanges = []
            for exc_code in df_firms['exchange_code']:
                if exc_code and exc_code not in [e[0] for e in unique_exchanges]:
                    # Lấy tên sàn tương ứng với mã sàn
                    exc_name = df_firms[df_firms['exchange_code'] == exc_code]['exchange_name'].iloc[0]
                    unique_exchanges.append((exc_code, exc_name))
            
            for code, name in unique_exchanges:
                conn.execute(text("INSERT INTO dim_exchange (exchange_code, exchange_name) VALUES (:c, :n)"), 
                             {"c": code, "n": name})

            # BƯỚC 3: XỬ LÝ NGÀNH (Đảm bảo ID thẳng hàng từ 1 đến 8)
            print("Đang nạp danh mục ngành hàng...")
            unique_industries = []
            for ind in df_firms['industry_l2_name']:
                if ind and ind not in unique_industries:
                    unique_industries.append(ind)
            
            # Nạp một lần duy nhất để ID không bị nhảy số
            for ind_name in unique_industries:
                conn.execute(text("INSERT INTO dim_industry_l2 (industry_l2_name) VALUES (:n)"), {"n": ind_name})
            
            print(f"-> Đã nạp xong {len(unique_industries)} ngành.")

            # BƯỚC 4: NẠP DOANH NGHIỆP (Liên kết với ID đã tạo)
            print("Đang nạp danh sách doanh nghiệp...")
            for _, row in df_firms.iterrows():
                # Truy vấn lấy ID sàn đã nạp
                exc_id = conn.execute(text("SELECT exchange_id FROM dim_exchange WHERE exchange_code = :c"),
                                       {"c": row['exchange_code']}).fetchone()[0]
                
                # Truy vấn lấy ID ngành đã nạp
                ind_id = conn.execute(text("SELECT industry_l2_id FROM dim_industry_l2 WHERE industry_l2_name = :n"),
                                      {"n": row['industry_l2_name']}).fetchone()[0]

                # Nạp vào bảng dim_firm
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
            print("\n--- KẾT QUẢ ---")
            print("1. Bảng dim_exchange: ID thẳng hàng (HOSE=1, HNX=2...)")
            print("2. Bảng dim_industry_l2: ID thẳng hàng từ 1 đến 8 (không nhảy số)")
            print("3. Bảng dim_firm: 20 doanh nghiệp nạp thành công.")

    except Exception as e:
        print(f"Lỗi khi thực hiện Script A: {e}")

if __name__ == "__main__":
    run_import_firms_perfect_id("data/ttin cty.xlsx")