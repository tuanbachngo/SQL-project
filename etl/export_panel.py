import pandas as pd
import mysql.connector
import os

def export_to_csv():
    # 1. Kết nối đến Database
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='your_password',
        database='vn_firm_panel'
    )
    
    # 2. Truy vấn dữ liệu từ View (Nơi đã gộp 39 trường và xử lý Override)
    query = "SELECT * FROM vw_firm_panel_latest"
    
    print("Đang trích xuất dữ liệu từ hệ thống...")
    df = pd.read_sql(query, conn)
    
    # 3. Hậu xử lý (Post-processing)
    # Sắp xếp theo Ticker và Năm để ra đúng cấu trúc Panel Data
    df = df.sort_values(by=['ticker', 'fiscal_year'])
    
    # Đảm bảo các trường tên biến khớp với danh sách 30-39 biến của bạn
    # (Ví dụ: net_sales_revenue, total_assets, ...)
    
    # 4. Xuất file
    output_path = 'outputs/final_firm_panel_dataset.csv'
    if not os.path.exists('outputs'): os.makedirs('outputs')
    
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    conn.close()
    print(f"Xuất dữ liệu thành công! File lưu tại: {output_path}")

if __name__ == "__main__":
    export_to_csv()