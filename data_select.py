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
    "total_share_outstanding": (
        "Số cổ phiếu đang lưu hành | số lượng cổ phiếu lưu hành | "
        "số cổ phần | số cổ phiếu phát hành (nếu không có 'lưu hành') "
        "[TM hoặc phần 'Cổ phiếu']"
    ),

    "total_sales_revenue": (
        "Doanh thu bán hàng và cung cấp dịch vụ (tổng) | "
        "Doanh thu thuần + các khoản giảm trừ (nếu chỉ có doanh thu thuần) "
        "[KQKD]"
    ),

    "net_sales_revenue": (
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

    "intangible_assets_value": (
        "Tài sản cố định vô hình (giá trị còn lại) | TSCĐ vô hình | "
        "Tài sản vô hình (giá trị còn lại) "
        "[BCĐKT, TM]"
    ),

    "manufacturing_overhead": (
        "Chi phí sản xuất chung | CPSXC | chi phí gián tiếp "
        "[TM giá thành/giá vốn, TM chi phí]"
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

    "merchandise_purchase": (
        "Mua hàng hóa | Giá mua hàng hóa | mua hàng trong kỳ "
        "[TM giá vốn/hàng tồn kho]"
    ),

    "wip_goods_purchase": (
        "Chi phí sản xuất, kinh doanh dở dang phát sinh | "
        "chi phí SXKD dở dang | dở dang (WIP) "
        "[BCĐKT, TM hàng tồn kho/giá thành]"
    ),

    "outside_manufacturing_expenses": (
        "Chi phí gia công | gia công thuê ngoài | chi phí thuê ngoài "
        "[TM giá thành/giá vốn]"
    ),

    "production_cost": (
        "Giá vốn hàng bán | Giá vốn | chi phí sản xuất (nếu trình bày dạng yếu tố) "
        "[KQKD, TM]"
    ),

    "rnd_expenditure": (
        "Chi phí nghiên cứu và phát triển | Chi phí R&D | Nghiên cứu phát triển "
        "[KQKD, TM]"
    ),

    "net_income": (
        "Lợi nhuận sau thuế thu nhập doanh nghiệp | LNST | "
        "Lợi nhuận sau thuế "
        "[KQKD]"
    ),

    "total_shareholders_equity": (
        "VỐN CHỦ SỞ HỮU | Tổng vốn chủ sở hữu | "
        "Nguồn vốn chủ sở hữu "
        "[BCĐKT]"
    ),

    "total_liabilities": (
        "NỢ PHẢI TRẢ | Tổng nợ phải trả "
        "[BCĐKT]"
    ),

    "net_cash_from_operating": (
        "Lưu chuyển tiền thuần từ hoạt động kinh doanh | "
        "Tiền thuần từ HĐKD "
        "[LCTT]"
    ),

    "capital_expenditure": (
        "Tiền chi để mua sắm, xây dựng TSCĐ và các tài sản dài hạn khác | "
        "Chi mua sắm TSCĐ | Chi đầu tư TSCĐ (CAPEX) "
        "[LCTT]"
    ),

    "cash_flows_from_investing": (
        "Lưu chuyển tiền thuần từ hoạt động đầu tư | "
        "Tiền thuần từ HĐ đầu tư "
        "[LCTT]"
    ),

    "cash_and_cash_equivalent": (
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

    "total_inventory": (
        "Hàng tồn kho | Tổng hàng tồn kho "
        "[BCĐKT]"
    ),

    "dividend_payment": (
        "Cổ tức đã trả | Chi trả cổ tức | Tiền chi trả cổ tức "
        "[LCTT, TM]"
    ),

    "eps": (
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

def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _guess_company_from_text(text: str) -> Optional[str]:
    m = re.search(r"\b([A-Z]{3,5})\b", text[:2000])
    return m.group(1) if m else None

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
    # 1) init client (giữ cách init giống hàm md_to_csv)
    client = genai.Client(
        vertexai=True,
        project=VERTEX_PROJECT if VERTEX_PROJECT else None,
        location=VERTEX_LOCATION,
    )

    # 2) schema giữ nguyên như md_to_csv (copy y hệt response_schema)
    response_schema = {
        "type": "object",
        "properties": {
            "company": {"type": "string"},
            "records": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "year": {"type": "integer"},
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
                    "required": ["year", "items"],
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
- DO NOT hallucinate, guess, estimate, or compute values. Only copy exact values appearing in the PDF.
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

    # 3) đọc bytes PDF và gửi thẳng vào Gemini
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

    # 4) ghi CSV: giữ nguyên logic write CSV y như md_to_csv
    os.makedirs(os.path.dirname(os.path.abspath(csv_path)), exist_ok=True)
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["company", "year", "key", "label", "value", "unit", "currency", "source_snippet", "confidence"])

        for rec in data.get("records", []):
            year = rec.get("year", "")
            for item in rec.get("items", []):
                writer.writerow([
                    company or data.get("company", ""),
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
