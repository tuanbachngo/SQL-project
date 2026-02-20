import pandas as pd
from vnstock import Quote # Sử dụng lớp Quote trực tiếp
import os

# 1. Danh sách 20 mã cổ phiếu
TICKERS = [
    "GVR", "BSR", "GEE", "DGC", "GMD", "VGC", "PVS", "DPM", "VCG", "PVD",
    "DHG", "VHC", "DBC", "PC1", "VSC", "NKG", "BHN", "TMS", "RAL", "PVC"
]

# 2. Cấu hình thời gian
START_DATE = "2020-12-01"
END_DATE = "2024-12-31"

def fetch_year_end_prices():
    all_prices = []
    print(f"--- Đang lấy dữ liệu giá ---")

    for ticker in TICKERS:
        try:
            print(f"Đang xử lý: {ticker}...", end=" ")
            q = Quote(symbol=ticker, source='VCI') 
            df = q.history(start=START_DATE, end=END_DATE, interval='1D')

            if df is not None and not df.empty:
                df['time'] = pd.to_datetime(df['time'])
                
                # Lấy ngày giao dịch cuối cùng của mỗi năm
                year_end_df = df.sort_values('time').groupby(df['time'].dt.year).tail(1)

                for _, row in year_end_df.iterrows():
                    all_prices.append({
                        'ticker': ticker,
                        'fiscal_year': row['time'].year,
                        'trading_date': row['time'].strftime('%Y-%m-%d'),
                        'share_price': row['close'] 
                    })
                print("Thành công.")
            else:
                print("Không có dữ liệu.")
                
        except Exception as e:
            print(f"Lỗi: {e}")

    # Chuyển kết quả thành DataFrame
    result_df = pd.DataFrame(all_prices)
        
    output_path = "external_share_prices.csv"
    result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print("\n--- Hoàn tất ---")
    print(f"File kết quả đã được lưu tại: {output_path}")
    print(result_df.head(10)) 

if __name__ == "__main__":
    fetch_year_end_prices()