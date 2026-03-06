from sqlalchemy import create_engine
import pandas as pd

# 1. Thông tin cấu hình duy nhất
DB_USER = "root"
DB_PASS = "Bachbachbach2006."  
DB_HOST = "localhost"
DB_NAME = "vn_firm_panel_test"

# 2. Tạo Engine (Chuỗi kết nối chuẩn)
# Cú pháp: mysql+mysqlconnector://user:password@host/database
DATABASE_URL = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

# Tạo engine với cơ chế Pool (giữ kết nối luôn sẵn sàng)
engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600
)

print(f"✅ Engine đã sẵn sàng kết nối tới: {DB_NAME}")

def get_engine():
    return engine