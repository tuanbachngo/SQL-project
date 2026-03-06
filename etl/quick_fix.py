import pandas as pd
from sqlalchemy import text
from datetime import datetime
import os
from database_setup import engine

EXCEL_PATH = "Final Gộp 39 trường dữ liệu (FINAL 3).xlsx"
SHEET_NAME = "master_39"
CSV_PATH = "qc_report_final.csv"
COLUMN_TYPE_MAP = {
    # Nhóm SỐ NGUYÊN (Integer)
    'fiscal_year': 'int',
    'shares_outstanding': 'int',
    'employees_count': 'int',
    'firm_age': 'int',
    'product_innovation': 'int',
    'process_innovation': 'int',
    'snapshot_id': 'int',
    'firm_id': 'int',

    # Nhóm SỐ THỰC (Float - Tỷ lệ hoặc Tiền tệ)
    'managerial_inside_own': 'float',
    'state_own': 'float',
    'institutional_own': 'float',
    'foreign_own': 'float',
    'share_price': 'float',
    'market_value_equity': 'float',
    'net_sales': 'float',
    'total_assets': 'float',
    'total_equity': 'float',
    'total_liabilities': 'float',
    'growth_ratio': 'float',
    'dividend_cash_paid': 'float',
    'eps_basic': 'float',
    # ... thêm các cột tài chính khác vào đây ...

    # Nhóm VĂN BẢN (String)
    'ticker': 'str',
    'evidence_note': 'str',
    'company_name': 'str'
}

# --- HÀM PHỤ TRỢ: ÉP KIỂU THÔNG MINH ---
def smart_parse_value(column_name, val):
    """
    Ép kiểu giá trị dựa trên bản đồ COLUMN_TYPE_MAP đã định nghĩa.
    """
    # 1. Xử lý giá trị trống
    if pd.isna(val) or str(val).lower() in ['none', 'nan', '', 'null', 'x']:
        return None
    val_str = str(val).strip()
    # 2. Lấy kiểu dữ liệu đích từ Mapping (mặc định là 'str' nếu không có trong map)
    target_type = COLUMN_TYPE_MAP.get(column_name, 'str')
    try:
        if target_type == 'int':
            # Chuyển string -> float -> int để xử lý trường hợp "500.0"
            return int(float(val_str)) 
        elif target_type == 'float':
            return float(val_str)   
        else:
            return val_str         
    except (ValueError, TypeError):
        # Nếu không thể ép kiểu (ví dụ nhập chữ "abc" vào cột sales), trả về chuỗi gốc
        return val_str

def apply_quick_fix(engine, ticker, fiscal_year, table_name, column_name, raw_value, reason, user_name="Group_Member"):
    try:
        with engine.begin() as conn:
            # --- BƯỚC 1: LẤY THÔNG TIN CƠ BẢN ---
            res_firm = conn.execute(text("SELECT firm_id FROM dim_firm WHERE ticker = :t"), {"t": ticker}).fetchone()
            if not res_firm:
                print(f"❌ Không tìm thấy mã {ticker}")
                return
            firm_id = res_firm[0]

            query_old = text(f"SELECT {column_name}, snapshot_id FROM {table_name} "
                             f"WHERE firm_id = :fid AND fiscal_year = :fy "
                             f"ORDER BY snapshot_id DESC LIMIT 1")
            res_old = conn.execute(query_old, {"fid": firm_id, "fy": fiscal_year}).fetchone()
            
            if not res_old:
                print(f"❌ Không tìm thấy dữ liệu cho {ticker} năm {fiscal_year} trong bảng {table_name}")
                return
            
            old_value, latest_snap_id = res_old[0], res_old[1]

            # --- BƯỚC 2: XỬ LÝ EXCEL TRƯỚC ĐỂ LẤY DTYPE ---
            if os.path.exists(EXCEL_PATH):
                df_excel = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
                final_value = smart_parse_value(column_name, raw_value)

                # Cập nhật Database với giá trị đã ép kiểu
                update_sql = text(f"UPDATE {table_name} SET {column_name} = :val "
                                  f"WHERE firm_id = :fid AND fiscal_year = :fy AND snapshot_id = :sid")
                conn.execute(update_sql, {"val": final_value, "fid": firm_id, "fy": fiscal_year, "sid": latest_snap_id})

                # Ghi Log
                log_sql = text("""
                    INSERT INTO fact_value_override_log 
                    (firm_id, fiscal_year, table_name, column_name, old_value, new_value, reason, changed_by, changed_at)
                    VALUES (:fid, :fy, :tbl, :col, :old, :new, :re, :user, :now)
                """)
                conn.execute(log_sql, {
                    "fid": firm_id, "fy": fiscal_year, "tbl": table_name, "col": column_name,
                    "old": str(old_value), "new": str(final_value), "re": reason, 
                    "user": user_name, "now": datetime.now()
                })
                print(f"✅ Đã cập nhật Database và ghi log cho {ticker} - {fiscal_year}")
                
                if column_name not in df_excel.columns:
                    print(f"➕ Cột '{column_name}' chưa tồn tại trong Excel. Đang tự động tạo mới...")
                    df_excel[column_name] = pd.NA  
                    df_excel[column_name] = df_excel[column_name].astype(object) 
                # Cập nhật Excel
                mask = (df_excel['ticker'].astype(str).str.strip().str.upper() == str(ticker).strip().upper()) & \
                       (df_excel['fiscal_year'].astype(float).astype(int) == int(fiscal_year))
                
                if mask.any():
                    df_excel[column_name] = df_excel[column_name].astype(object)
                    df_excel.loc[mask, column_name] = final_value
                    df_excel.to_excel(EXCEL_PATH, sheet_name=SHEET_NAME, index=False)
                    print(f"✅ Đã đồng bộ số liệu ({final_value}) vào file Excel.")
                else:
                    print(f"⚠️ Cảnh báo: Không tìm thấy dòng tương ứng trong Excel.")
            else:
                print(f"⚠️ Cảnh báo: File Excel gốc không tồn tại.")

    except Exception as e:
        print(f"❌ Lỗi: {e}")

def main():
    # Đọc CSV chứa các dòng cần sửa
    df_fixes = pd.read_csv(CSV_PATH)

    for index, row in df_fixes.iterrows():
        ticker = str(row['ticker']).strip()
        
        # --- 1. KIỂM TRA TÊN BẢNG (Chặn lỗi .nan) ---
        table_name = str(row['table_name']).strip()
        if table_name.lower() == 'nan' or not table_name:
            print(f"Bỏ qua dòng {index} [{ticker}]: table_name bị trống (NaN)")
            continue

        # --- 2. KIỂM TRA TÊN CỘT (Chặn lỗi nhiều hơn 1 tên) ---
        column_name = str(row['column_name']).strip()
        # Nếu tên cột chứa các ký tự phân tách như / , ; hoặc khoảng trắng
        if any(char in column_name for char in ['/', ',', ';', ' ']):
            print(f"Bỏ qua dòng {index} [{ticker}]: column_name chứa nhiều hơn 1 cột ('{column_name}')")
            continue
        
        if column_name.lower() == 'nan' or not column_name:
            print(f"Bỏ qua dòng {index} [{ticker}]: column_name bị trống")
            continue

        # --- 3. KIỂM TRA NĂM (Chặn lỗi chuỗi '2020-2022') ---
        try:
            # Ép kiểu về float rồi mới sang int để xử lý các số dạng '2022.0'
            fiscal_year = int(float(row['fiscal_year']))
        except (ValueError, TypeError):
            print(f"Bỏ qua dòng {index} [{ticker}]: fiscal_year '{row['fiscal_year']}' không phải là số đơn lẻ")
            continue

        # --- 4. KIỂM TRA GIÁ TRỊ MỚI ---
        new_value = row['new_value']
        if pd.isna(new_value) or str(new_value).strip() == "":
            print(f"Bỏ qua dòng {index} [{ticker}]: Không có giá trị mới để cập nhật.")
            continue

        # --- NẾU VƯỢT QUA TẤT CẢ CÁC BƯỚC TRÊN -> CHẠY FIX ---
        apply_quick_fix(engine, ticker, fiscal_year, table_name, column_name, new_value, row['message'])
            
if __name__ == "__main__":
    main()