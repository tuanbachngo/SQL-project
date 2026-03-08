from __future__ import annotations

import io
import csv
import json
import os
import re
from typing import Dict, List, Optional

from google import genai
from google.genai import types

MODEL_ID = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")

VERTEX_LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "global")

VERTEX_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "")

DATA_POINTS: Dict[str, str] = {
    "shares_outstanding": (
        "Số cổ phiếu đang lưu hành | số lượng cổ phiếu lưu hành | "
        "số cổ phần | số cổ phiếu phát hành (nếu không có 'lưu hành') "
        "[TM hoặc phần 'Cổ phiếu']"
    ),

    "total_sales_revenue": (
        "Doanh thu bán hàng và cung cấp dịch vụ (tổng) | "
        "Doanh thu thuần + các khoản giảm trừ (nếu chỉ có doanh thu thuần) "
        "[KQKD]"
    ),

    "net_sales": (
        "Doanh thu thuần về bán hàng và cung cấp dịch vụ | Doanh thu thuần "
        "[KQKD]"
    ),

    "total_assets": (
        "TỔNG CỘNG TÀI SẢN | Tổng tài sản "
        "[BCĐKT]"
    ),

    "selling_expenses": (
        "Chi phí bán hàng "
        "[KQKD]"
    ),

    "general_admin_expenses": (
        "Chi phí quản lý doanh nghiệp | Chi phí QLDN "
        "[KQKD]"
    ),

    "intangible_assets_net": (
        "Tài sản cố định vô hình (giá trị còn lại) | TSCĐ vô hình | "
        "Tài sản vô hình (giá trị còn lại) "
        "[BCĐKT, TM]"
    ),

    "manufacturing_overhead": (
        "Chi phí sản xuất chung | CPSXC | MOH | Manufacturing Overhead",
        "Logic: [Tại bảng 'Chi phí sản xuất kinh doanh theo yếu tố', lấy dòng 'Tổng cộng' trừ đi dòng 'Chi phí nguyên liệu, vật liệu' và dòng 'Chi phí nhân công']. Lưu ý: Thực hiện tính toán riêng biệt cho từng cột (Năm nay/Năm trước) để trả về kết quả tương ứng."
    ),

    "net_operating_income": (
        "Lợi nhuận thuần từ hoạt động kinh doanh | Lợi nhuận thuần HĐKD | "
        "Lợi nhuận từ hoạt động kinh doanh "
        "[KQKD]"
    ),

    "raw_material_consumption": (
        "Chi phí nguyên vật liệu trực tiếp | NVL trực tiếp | "
        "Chi phí nguyên vật liệu | tiêu hao nguyên vật liệu "
        "[TM giá thành/giá vốn]"
    ),

    "merchandise_purchase_year": (
        "Giá trị hàng nhập mua và sản xuất trong kỳ | Tổng giá trị đầu vào "
        "[Công thức: Giá vốn hàng bán (Mã 11) + Hàng tồn kho cuối kỳ (Mã 140 tại cột cuối kỳ năm nay) - Hàng tồn kho đầu kỳ (Mã 140 tại cột cuối kỳ năm trước|Mã 140 tại cột đầu kỳ năm nay)]"
    ),      

    "wip_goods_purchase": (
        "Chi phí sản xuất, kinh doanh dở dang"
        "chi phí SXKD dở dang | dở dang (WIP) "
        "[BCĐKT, Tài sản dở dang dài hạn]"
    ),

    "outside_manufacturing_expenses": (
        "chi phí dịch vụ mua ngoài | gia công thuê ngoài | Chi phí gia công "
        "[TM chi phí sản xuất kinh doanh theo yếu tố]"
    ),

    "production_cost": (
        "Tổng cộng chi phí sản xuất kinh doanh theo yếu tố | Tổng cộng | Total production and business costs by element",
        "[TM chi phí sản xuất kinh doanh theo yếu tố (dòng Tổng cộng)]"
    ),

    "rnd_expenses": (
    "Chi sử dụng quỹ phát triển KH&CN | Chi phí R&D "
    "[LOGIC ƯU TIÊN: "
        "1. Ưu tiên Thuyết minh 'Quỹ phát triển khoa học và công nghệ': "
        "trích xuất DUY NHẤT giá trị tại dòng '- Sử dụng quỹ' (hoặc các biến thể tên tương đương như 'Sử dụng quỹ', 'Chi từ quỹ', 'Chi/ sử dụng quỹ KH&CN'). "
        "KHÔNG dùng dòng 'Giảm trong năm' nếu đó là tổng (có thể bao gồm khoản khác ngoài sử dụng quỹ). "
        "2. Nếu không có dòng '- Sử dụng quỹ' trong thuyết minh: trả về NA/None (không suy diễn từ (cuối năm - đầu năm))."
    "]"
    ),

    "net_income": (
        "Lợi nhuận sau thuế thu nhập doanh nghiệp | LNST | "
        "Lợi nhuận sau thuế "
        "[KQKD]"
    ),

    "total_equity": (
        "VỐN CHỦ SỞ HỮU | Tổng vốn chủ sở hữu | "
        "Nguồn vốn chủ sở hữu "
        "[BCĐKT]"
    ),

    "total_liabilities": (
        "NỢ PHẢI TRẢ | Tổng nợ phải trả "
        "[BCĐKT]"
    ),

    "net_cfo": (
        "Lưu chuyển tiền thuần từ hoạt động kinh doanh | "
        "Tiền thuần từ HĐKD "
        "[LCTT]"
    ),

    "capex": (
        "Tiền chi để mua sắm, xây dựng TSCĐ và các tài sản dài hạn khác | "
        "Chi mua sắm TSCĐ | Chi đầu tư TSCĐ (CAPEX) "
        "[LCTT]"
    ),

    "net_cfi": (
        "Lưu chuyển tiền thuần từ hoạt động đầu tư | "
        "Tiền thuần từ HĐ đầu tư "
        "[LCTT]"
    ),

    "cash_and_equivalents": (
        "Tiền và các khoản tương đương tiền | Tiền và tương đương tiền "
        "[BCĐKT]"
    ),

    "long_term_debt": (
        "Vay và nợ thuê tài chính dài hạn | Vay dài hạn | Nợ vay dài hạn "
        "[BCĐKT, TM]"
    ),

    "current_assets": (
        "TÀI SẢN NGẮN HẠN | Tổng tài sản ngắn hạn "
        "[BCĐKT]"
    ),

    "current_liabilities": (
        "NỢ NGẮN HẠN | Tổng nợ ngắn hạn "
        "[BCĐKT]"
    ),

    "growth_ratio": (
        "Tăng/giảm (%) so với năm trước | Tỷ lệ tăng trưởng | Growth (%) "
        "[TM hoặc phần chỉ tiêu/biểu đồ nếu có]"
    ),

    "inventory": (
        "Hàng tồn kho | Tổng hàng tồn kho "
        "[BCĐKT]"
    ),

    "dividend_cash_paid": (
        "Cổ tức đã trả | Chi trả cổ tức | Tiền chi trả cổ tức "
        "[LCTT, TM]"
    ),

    "eps_basic": (
        "Lãi cơ bản trên cổ phiếu | EPS cơ bản | "
        "Lãi trên cổ phiếu "
        "[KQKD]"
    ),

    "net_ppe": (
        "Tài sản cố định hữu hình (giá trị còn lại) | TSCĐ hữu hình | "
        "PPE (giá trị còn lại) "
        "[BCĐKT, TM]"
    ),
}

def extract_financial_data_pdf_to_csv(
    pdf_path: str,
    csv_path: str,
    *,
    company: str = "",
    years: Optional[List[int]] = None,
    currency_hint: str = "VND",
    unit_hint: str = "as stated in the report (e.g. VND, thousand VND, million VND)",
    temperature: float = 0.0,
) -> str:
    client = genai.Client(
        vertexai=True,
        project=VERTEX_PROJECT if VERTEX_PROJECT else None,
        location=VERTEX_LOCATION,
    )

    response_schema = {
        "type": "object",
        "properties": {
            "ticker": {"type": "string"},
            "records": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "fiscal_year": {"type": "integer"},
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "key": {"type": "string"},
                                    "label": {"type": "string"},
                                    "value": {"type": "string"},
                                    "unit": {"type": "string"},
                                    "currency": {"type": "string"},
                                    "source_snippet": {"type": "string"},
                                    "confidence": {"type": "number"},
                                },
                                "required": ["key", "label", "value"],
                            },
                        },
                    },
                    "required": ["fiscal_year", "items"],
                },
            },
        },
        "required": ["records"],
    }

    years_txt = f"\nOnly extract for these years: {years}.\n" if years else "\nIf multiple years exist, extract each year separately.\n"
    dp_lines = "\n".join([f"- {k}: {v}" for k, v in DATA_POINTS.items()])

    prompt = f"""
You are a financial data extraction system.

TASK:
You will be given a PDF of financial statements.
Extract the following data points and return ONLY valid JSON matching the schema.

DATA POINTS (key -> meaning):
{dp_lines}

RULES:
- Output in records grouped by year.
- If a value is missing/unknown, set value to "".
- DO NOT hallucinate or guess. If a formula is provided in the data point description (in brackets []), perform that calculation based on exact values from the PDF. Otherwise, copy exact values.
- CRITICAL: For any non-empty value, source_snippet MUST contain the same digits as value (ignore spaces and thousand separators like "," ".").
- For EACH year, you MUST return ALL keys listed in DATA_POINTS (one item per key), in the same order.
- If a key is not found, still output that item with value="" , unit="", currency="", source_snippet="", confidence=0.
- Keep the number formatting exactly as in the report if possible.
- Provide unit and currency if stated; else empty.
- source_snippet: short snippet (<= 200 chars) from the PDF content you used.
- confidence: float 0..1.
- currency_hint: {currency_hint}
- unit_hint: {unit_hint}
{years_txt}
""".strip()

    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()

    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part(text=prompt),
                types.Part(inline_data=types.Blob(mime_type="application/pdf", data=pdf_bytes)),
            ],
        )
    ]

    cfg = types.GenerateContentConfig(
        temperature=temperature,
        response_mime_type="application/json",
        response_schema=response_schema,
    )

    resp = client.models.generate_content(
        model=MODEL_ID,
        contents=contents,
        config=cfg,
    )

    data = json.loads(resp.text)
    os.makedirs(os.path.dirname(os.path.abspath(csv_path)), exist_ok=True)
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ticker", "fiscal_year", "key", "label", "value", "unit", "currency", "source_snippet", "confidence"])

        for rec in data.get("records", []):
            year = rec.get("fiscal_year", "")
            for item in rec.get("items", []):
                writer.writerow([
                    company or data.get("ticker", ""),
                    year,
                    item.get("key", ""),
                    item.get("label", ""),
                    item.get("value", ""),
                    item.get("unit", ""),
                    item.get("currency", ""),
                    item.get("source_snippet", ""),
                    item.get("confidence", ""),
                ])

    return csv_path
