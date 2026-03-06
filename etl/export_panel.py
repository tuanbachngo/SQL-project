import pandas as pd
import os
from database_setup import engine

def export_to_csv():
    query = "SELECT * FROM vw_firm_panel_latest where ticker <> 'TEST'"
    
    print("Đang trích xuất dữ liệu từ hệ thống...")
    df = pd.read_sql(query, engine)
    df = df.sort_values(by=['ticker', 'fiscal_year'])
    output_path = 'final_firm_panel_dataset.csv'
    
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    engine.dispose()

    print(f"Xuất dữ liệu thành công! File lưu tại: {output_path}")

if __name__ == "__main__":
    export_to_csv()