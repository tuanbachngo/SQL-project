import os
import io
import pypdfium2 as pdfium
from datetime import datetime
from google import genai
from google.genai import types


def pdf_to_markdown_gemini(
    pdf_path: str,
    *,
    model: str = "gemini-3-pro-preview",
    scale: float = 2.0,
    max_pages: int | None = None,
    api_key_env: str = "GOOGLE_CLOUD_API_KEY",
    temperature: float = 0.0,
    top_p: float = 0.95,
    thinking_level: str = "HIGH",
    stream: bool = True,
) -> str:
    
    # 1) Init Vertex AI client
    client = genai.Client(
        vertexai=True,
        api_key=os.environ.get(api_key_env),
    )

    # 2) Load PDF in memory and render pages
    pdf = pdfium.PdfDocument(pdf_path)
    total_pages = len(pdf)
    if max_pages is not None:
        total_pages = min(total_pages, max_pages)

    # 3) Output markdown header
    md_out = []
    md_out.append(f"# OCR Markdown Report")
    md_out.append("")
    md_out.append(f"- Source: `{os.path.basename(pdf_path)}`")
    md_out.append(f"- Processed at: `{datetime.utcnow().isoformat()}Z`")
    md_out.append(f"- Pages: `{total_pages}`")
    md_out.append(f"- Render scale: `{scale}`")
    md_out.append("")

    # 4) Shared config (based on your code)
    tools = [
        types.Tool(google_search=types.GoogleSearch()),
    ]

    generate_content_config = types.GenerateContentConfig(
        temperature=temperature,
        top_p=top_p,
        max_output_tokens=8192,  # an toàn hơn; 65535 dễ nổ/timeout
        safety_settings=[
            types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="OFF"),
            types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"),
            types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"),
            types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="OFF"),
        ],
        tools=tools,
        thinking_config=types.ThinkingConfig(thinking_level=thinking_level),
    )

    # 5) Prompt OCR -> markdown
    ocr_prompt = (
        "You are a strict OCR engine.\n"
        "Task: Transcribe ALL Vietnamese text from the image.\n"
        "Output MUST be raw Markdown (no ``` code fences, no explanations).\n"
        "Rules:\n"
        "- Preserve reading order.\n"
        "- Keep headings as Markdown headings.\n"
        "- If you detect a simple table, use a Markdown table.\n"
        "- If table is complex/merged cells, keep it as aligned text (do NOT invent data).\n"
        "- Do NOT repeat the same line many times; if duplicates occur, keep only one.\n"
        "- If unreadable, return IMAGE_UNREADABLE.\n"
    )

    # 6) Process each page
    for page_idx in range(total_pages):
        print(f"[Gemini OCR] Page {page_idx+1}/{total_pages}...")

        page = pdf[page_idx]
        bitmap = page.render(scale=scale)
        pil_image = bitmap.to_pil()

        buf = io.BytesIO()
        pil_image.save(buf, format="PNG")
        image_bytes = buf.getvalue()
        buf.close()

        # Build content parts: text + inline image bytes
        contents = [
            types.Content(
                role="user",
                parts=[
                    types.Part(text=ocr_prompt),
                    # Inline image bytes (PNG)
                    types.Part(inline_data=types.Blob(mime_type="image/png", data=image_bytes)),
                ],
            )
        ]

        # Add per-page heading in output
        md_out.append(f"## Page {page_idx+1}")
        md_out.append("")

        if stream:
            page_text_parts = []
            for chunk in client.models.generate_content_stream(
                model=model,
                contents=contents,
                config=generate_content_config,
            ):
                if not chunk.candidates or not chunk.candidates[0].content or not chunk.candidates[0].content.parts:
                    continue
                if chunk.text:
                    page_text_parts.append(chunk.text)
            page_md = "".join(page_text_parts).strip()
        else:
            resp = client.models.generate_content(
                model=model,
                contents=contents,
                config=generate_content_config,
            )
            page_md = (resp.text or "").strip()

        # Clean: remove accidental code fences if model disobeys
        page_md = page_md.replace("```markdown", "").replace("```", "").strip()

        md_out.append(page_md if page_md else "IMAGE_UNREADABLE")
        md_out.append("")
        md_out.append("---")
        md_out.append("")

    return "\n".join(md_out)

if __name__ == "__main__":
    # Example usage
    pdf_file = r"0004776021080368906tng-ctcp-bia-ru-nc-gii-kht-h-ni-habeco29032024-160930.pdf"
    md = pdf_to_markdown_gemini(pdf_file, scale=2.0, temperature=0.0)
    with open("output_report.md", "w", encoding="utf-8") as f:
        f.write(md)
    print("✅ Saved: output_report.md")
