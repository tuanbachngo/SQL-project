from google.cloud import storage
from pathlib import Path
import time
import tempfile

import torch
from transformers import AutoModel, AutoTokenizer
from pdf2image import convert_from_path, pdfinfo_from_path  


def download_pdf_from_gcs(
    bucket_name: str,
    blob_path: str,
    local_path: str,
) -> str:
    local_file = Path(local_path)
    local_file.parent.mkdir(parents=True, exist_ok=True)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    if not blob.exists(client=client):
        raise FileNotFoundError(f"GCS object not found: gs://{bucket_name}/{blob_path}")

    blob.download_to_filename(str(local_file))
    return str(local_file)


def deepseek_ocr2_pdf_to_text(pdf_path: str, output_dir: str, dpi: int = 200) -> str:
    pdf_path = Path(pdf_path)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    tokenizer = AutoTokenizer.from_pretrained("deepseek-ai/DeepSeek-OCR-2", trust_remote_code=True)
    model = AutoModel.from_pretrained("deepseek-ai/DeepSeek-OCR-2", trust_remote_code=True, use_safetensors=True)

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = model.eval().to(device)
    if device == "cuda":
        model = model.to(torch.float16)

    prompt = "<image>\n<|grounding|>Convert the document to plain text."

    info = pdfinfo_from_path(str(pdf_path))
    num_pages = info["Pages"]
    print(f"[INFO] PDF pages: {num_pages} | dpi={dpi} | device={device}")

    chunks = []
    start_all = time.time()
    per_page_times = []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        for i in range(1, num_pages + 1):
            t0 = time.time()

            img = convert_from_path(
                str(pdf_path),
                dpi=dpi,
                first_page=i,
                last_page=i,
            )[0]

            img_path = tmpdir / f"{pdf_path.stem}_page_{i:04d}.png"
            img.save(img_path, "PNG")

            res = model.infer(
                tokenizer,
                prompt=prompt,
                image_file=str(img_path),
                output_path=str(out),
                base_size=1024,
                image_size=768,
                crop_mode=True,
                save_results=True,  
            )

            text = res if isinstance(res, str) else (res.get("text") or res.get("markdown") or str(res))
            chunks.append(f"\n\n===== PAGE {i} =====\n{text}")

            dt = time.time() - t0
            per_page_times.append(dt)
            avg = sum(per_page_times) / len(per_page_times)
            remaining = num_pages - i
            eta_sec = remaining * avg
            elapsed = time.time() - start_all

            print(f"[{i}/{num_pages}] page_time={dt:.1f}s | avg={avg:.1f}s/page | "
                  f"elapsed={elapsed/60:.1f}m | ETA={eta_sec/60:.1f}m")

    combined = "\n".join(chunks)
    (out / f"{pdf_path.stem}_deepseek_ocr2.txt").write_text(combined, encoding="utf-8")
    print(f"[DONE] Wrote: {out / f'{pdf_path.stem}_deepseek_ocr2.txt'}")

    return combined


if __name__ == "__main__":
    local_pdf = "/opt/airflow/ocr_input/BHN_20CN_BCTC_HNKT.pdf"

    download_pdf_from_gcs(
        bucket_name="raw_information",
        blob_path="BHN/BHN_20CN_BCTC_HNKT.pdf",
        local_path=local_pdf,
    )

    deepseek_ocr2_pdf_to_text(
        pdf_path=local_pdf,
        output_dir="/opt/airflow/ocr_output",
        dpi=200,
    )