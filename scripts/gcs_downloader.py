from __future__ import annotations

from pathlib import Path
from google.cloud import storage


def download_csvs(
    *,
    bucket_name: str,
    prefix: str,
    out_dir: str,
    skip_if_exists: bool = True,
):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    out_root = Path(out_dir).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    total = 0
    downloaded = 0
    skipped = 0

    # list objects theo prefix
    blobs = client.list_blobs(bucket_name, prefix=prefix)

    for blob in blobs:
        # bỏ folder markers
        if blob.name.endswith("/"):
            continue

        # chỉ tải .csv
        if not blob.name.lower().endswith(".csv"):
            continue

        total += 1

        # giữ nguyên cấu trúc thư mục theo blob.name
        local_path = out_root / blob.name
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if skip_if_exists and local_path.exists() and local_path.stat().st_size > 0:
            skipped += 1
            print(f"[SKIP] {blob.name}")
            continue

        blob.download_to_filename(str(local_path))
        downloaded += 1
        print(f"[OK] gs://{bucket_name}/{blob.name} -> {local_path}")

    print("\n==== SUMMARY ====")
    print(f"Matched CSV: {total}")
    print(f"Downloaded:  {downloaded}")
    print(f"Skipped:     {skipped}")
    print("Done.")


if __name__ == "__main__":
    # ====== CẤU HÌNH Ở ĐÂY ======
    BUCKET_NAME = "text_ocr_output"   # bucket chứa CSV output của bạn
    PREFIX = ""                      # ví dụ: "BHN_csv/" hoặc "" để lấy hết bucket
    OUT_DIR = "./downloaded_csv"     # thư mục lưu về máy
    SKIP_IF_EXISTS = True

    download_csvs(
        bucket_name=BUCKET_NAME,
        prefix=PREFIX,
        out_dir=OUT_DIR,
        skip_if_exists=SKIP_IF_EXISTS,
    )
