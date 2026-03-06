import pandas as pd
from datetime import datetime
from database_setup import engine
from sqlalchemy import text

def clean_date(x):
    if x is None or pd.isna(x):
        return None
    return pd.to_datetime(x).date().isoformat()

def create_snapshots_from_excel(excel_path):
    """
    Đọc cấu hình từ sheet version_info và tạo NHIỀU snapshot trong SQL.
    Trả về danh sách các snapshot_id đã tạo.
    """
    created_ids = []
    conn = None
    
    try:
        # Đọc sheet version_info
        df_ver = pd.read_excel(excel_path, sheet_name='version_info')
        df_ver = df_ver.where(pd.notnull(df_ver), None)
        
        if df_ver.empty:
            print("Lỗi: Sheet version_info không có dữ liệu!")
            return []

        with engine.begin() as conn:
            snapshot_date = datetime.now().strftime('%Y-%m-%d')
            print(f"--- Bắt đầu tạo snapshots (Tìm thấy {len(df_ver)} dòng) ---")

            # Bước 1: Vòng lặp qua từng dòng trong file Excel
            for index, config in df_ver.iterrows():
                source_name = config['source_name']
                fiscal_year = int(config['fiscal_year'])
                p_from = clean_date(config.get('period_from'))
                p_to   = clean_date(config.get('period_to'))
                version_tag = config['version_tag']
                created_by  = "Group_Member"

                # 1.1 Tra cứu source_id cho từng dòng
                result = conn.execute(text("SELECT source_id FROM dim_data_source WHERE source_name = :name"), {"name": source_name}).fetchone()
                
                if not result:
                    print(f"Bỏ qua dòng {index+1}: Nguồn '{source_name}' chưa có trong dim_data_source!")
                    continue
                
                source_id = result[0]

                # 1.2 Chèn vào fact_data_snapshot
                query = text("""
                    INSERT INTO fact_data_snapshot 
                    (snapshot_date, fiscal_year, period_from, period_to, source_id, version_tag, created_by)
                    VALUES (:s_date, :f_year, :p_from, :p_to, :s_id, :tag, :user)
                """)
                values = {
                    "s_date": snapshot_date,
                    "f_year": fiscal_year,
                    "p_from": p_from,
                    "p_to": p_to,
                    "s_id": source_id,
                    "tag": version_tag,
                    "user": created_by
                }
                
                result = conn.execute(query, values)
                new_id = result.lastrowid
                created_ids.append(new_id)
                
                print(f"Dòng {index+1}: Đã tạo Snapshot ID {new_id} ({source_name} - {fiscal_year})")

            print(f"--- HOÀN THÀNH: Đã tạo tổng cộng {len(created_ids)} snapshots ---")
            return created_ids

    except Exception as e:
        print(f"Lỗi khi thực hiện Script B: {e}")
        if conn:
            conn.rollback()
        return []

if __name__ == "__main__":
    excel_file = "ttin cty.xlsx"
    snapshot_ids = create_snapshots_from_excel(excel_file)
    
    if snapshot_ids:
        print(f"\n=> Các ID sẵn sàng: {snapshot_ids}")
        print(f"Gợi ý: Nếu bạn nạp dữ liệu cho nhiều năm, hãy dùng lần lượt các ID này.")
