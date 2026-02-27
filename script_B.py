import mysql.connector
from datetime import datetime

# 1. Cấu hình kết nối MySQL
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Binhthaii0203=))",
    "database": "vn_firm_panel_test"
}

def create_snapshot(source_name, fiscal_year, p_from, p_to, version_tag, created_by="Group_Member"):
    """
    Tạo một snapshot mới với khoảng thời gian period_from và period_to.
    """
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    try:
        # Bước 1: Tra cứu source_id
        cursor.execute("SELECT source_id FROM dim_data_source WHERE source_name = %s", (source_name,))
        result = cursor.fetchone()
        
        if not result:
            print(f"Lỗi: Nguồn dữ liệu '{source_name}' không tồn tại!")
            return None
        
        source_id = result[0]
        snapshot_date = datetime.now().strftime('%Y-%m-%d')

        # Bước 2: Chèn bản ghi mới vào fact_data_snapshot
        # Sử dụng đúng tên cột period_from và period_to
        query = """
            INSERT INTO fact_data_snapshot 
            (snapshot_date, fiscal_year, period_from, period_to, source_id, version_tag, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        values = (snapshot_date, fiscal_year, p_from, p_to, source_id, version_tag, created_by)
        
        cursor.execute(query, values)
        conn.commit()
        
        new_snapshot_id = cursor.lastrowid
        
        print(f"--- ĐÃ TẠO SNAPSHOT THÀNH CÔNG ---")
        print(f"Snapshot ID : {new_snapshot_id}")
        print(f"Khoảng thời gian: {p_from} đến {p_to}")
        print(f"Năm tài chính : {fiscal_year}")
        print(f"Phiên bản      : {version_tag}")
        
        return new_snapshot_id

    except mysql.connector.Error as err:
        print(f"Lỗi Database: {err}")
        return None
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # VÍ DỤ CÁCH ĐIỀN:
    # Nếu là báo cáo năm 2024: From là 01-01, To là 31-12
    # Định dạng ngày nên là 'YYYY-MM-DD' để SQL hiểu đúng
    
    new_id = create_snapshot(
        source_name="Vietstock", 
        fiscal_year=2024, 
        p_from="2024-01-01", 
        p_to="2024-12-31", 
        version_tag="v1.0_initial"
    )
    
    if new_id:
        print(f"\n=> Hãy dùng ID {new_id} này cho Script C.")