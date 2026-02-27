import pandas as pd
import mysql.connector
from datetime import datetime

# 1. Cấu hình kết nối MySQL
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Binhthaii0203=))",
    "database": "vn_firm_panel_test"
}

def create_snapshot_from_excel(excel_path):
    """
    Đọc cấu hình từ sheet version_info và tạo snapshot trong SQL.
    """
    try:
        # Đọc sheet version_info
        df_ver = pd.read_excel(excel_path, sheet_name='version_info')
        
        # Xử lý giá trị trống (NaN)
        df_ver = df_ver.where(pd.notnull(df_ver), None)
        
        # Lấy dòng đầu tiên của sheet để làm thông tin snapshot
        if df_ver.empty:
            print("Lỗi: Sheet version_info không có dữ liệu!")
            return None
        
        config = df_ver.iloc[0] # Lấy dòng đầu tiên
        
        source_name = config['source_name']
        fiscal_year = int(config['fiscal_year'])
        p_from      = str(config['period_from'])
        p_to        = str(config['period_to'])
        version_tag = config['version_tag']
        created_by  = "Group_Member"

        # Kết nối Database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Bước 1: Tra cứu source_id
        cursor.execute("SELECT source_id FROM dim_data_source WHERE source_name = %s", (source_name,))
        result = cursor.fetchone()
        
        if not result:
            print(f"Lỗi: Nguồn '{source_name}' chưa có trong dim_data_source. Hãy chạy Script A trước!")
            return None
        
        source_id = result[0]
        snapshot_date = datetime.now().strftime('%Y-%m-%d')

        # Bước 2: Chèn vào fact_data_snapshot
        query = """
            INSERT INTO fact_data_snapshot 
            (snapshot_date, fiscal_year, period_from, period_to, source_id, version_tag, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        values = (snapshot_date, fiscal_year, p_from, p_to, source_id, version_tag, created_by)
        
        cursor.execute(query, values)
        conn.commit()
        
        new_id = cursor.lastrowid
        
        print(f"--- ĐÃ TẠO SNAPSHOT TỪ EXCEL THÀNH CÔNG ---")
        print(f"Snapshot ID : {new_id}")
        print(f"Thông tin   : {source_name} | {fiscal_year} | {version_tag}")
        
        return new_id

    except Exception as e:
        print(f"Lỗi khi thực hiện Script B: {e}")
        return None
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    # Đường dẫn file chứa 3 sheet: company_info, source_info, version_info
    excel_file = "data/ttin cty.xlsx"
    
    snapshot_id = create_snapshot_from_excel(excel_file)
    
    if snapshot_id:
        print(f"\n=> Pipeline Ready: Hãy dùng ID {snapshot_id} cho Script C.")