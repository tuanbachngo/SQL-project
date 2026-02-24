import mysql.connector
from datetime import datetime

# 1. Cấu hình kết nối MySQL (Thay đổi thông số cho khớp với máy của bạn)
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Binhthaii0203=))",
    "database": "vn_firm_panel_test"
}

def create_snapshot(source_name, fiscal_year, version_tag, created_by="Group_Member"):
    """
    Tạo một snapshot mới trong bảng fact_data_snapshot.
    Trả về snapshot_id nếu thành công.
    """
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    try:
        # Bước 1: Tra cứu source_id dựa trên source_name từ bảng dim_data_source
        # Tên này phải khớp chính xác với cột 'source_name' trong sheet source_info của bạn
        cursor.execute("SELECT source_id FROM dim_data_source WHERE source_name = %s", (source_name,))
        result = cursor.fetchone()
        
        if not result:
            print(f"Lỗi: Nguồn dữ liệu '{source_name}' không tồn tại trong danh mục dim_data_source!")
            print("Vui lòng chạy Script A trước hoặc kiểm tra lại sheet source_info.")
            return None
        
        source_id = result[0]
        snapshot_date = datetime.now().strftime('%Y-%m-%d')

        # Bước 2: Chèn bản ghi mới vào bảng fact_data_snapshot
        # Cấu trúc: (snapshot_date, fiscal_year, source_id, version_tag, created_by)
        query = """
            INSERT INTO fact_data_snapshot (snapshot_date, fiscal_year, source_id, version_tag, created_by)
            VALUES (%s, %s, %s, %s, %s)
        """
        values = (snapshot_date, fiscal_year, source_id, version_tag, created_by)
        
        cursor.execute(query, values)
        conn.commit()
        
        # Bước 3: Lấy ID vừa tự động sinh ra
        new_snapshot_id = cursor.lastrowid
        
        print(f"--- ĐÃ TẠO SNAPSHOT THÀNH CÔNG ---")
        print(f"Snapshot ID: {new_snapshot_id}")
        print(f"Nguồn: {source_name} | Năm tài chính: {fiscal_year} | Phiên bản: {version_tag}")
        
        return new_snapshot_id

    except mysql.connector.Error as err:
        print(f"Lỗi Database: {err}")
        return None
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Ví dụ thực tế dựa trên dữ liệu sheet source_info của bạn:
    # Bạn có thể chọn source_name là 'Vietstock', 'BCTC_Audited' hoặc 'BCTN'
    
    selected_source = "Combined_Source" 
    target_year = 2024
    v_tag = "v1.0_initial_import"
    
    new_id = create_snapshot(selected_source, target_year, v_tag)
    
    if new_id:
        print(f"\n=> Bạn hãy copy ID {new_id} này để nhập vào Script C.")