from __future__ import annotations

import os
import sys
from typing import Optional, List

from google.cloud import storage

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from collect_upload_gcs import list_pdfs_in_gcs
from processing import process_one_pdf_gcs_to_csv

SRC_BUCKET = "raw_information"
SRC_PREFIX = "" 
DEST_BUCKET = "text_ocr_output"

SKIP_IF_EXISTS = True

YEARS: Optional[List[int]] = None

def infer_ticker_from_gs_uri(gs_pdf_uri: str) -> str:
    assert gs_pdf_uri.startswith("gs://")
    path = gs_pdf_uri.split("gs://", 1)[1].split("/", 1)[1]  
    ticker = path.split("/", 1)[0]
    return ticker


def main():
    client = storage.Client()

    pdfs = list_pdfs_in_gcs(SRC_BUCKET, SRC_PREFIX, client)
    print(f"Found {len(pdfs)} PDFs")

    ok = 0
    failed = 0

    for i, gs_pdf_uri in enumerate(pdfs, 1):
        ticker = infer_ticker_from_gs_uri(gs_pdf_uri)
        dest_prefix = f"{ticker}_csv"

        try:
            out_uri = process_one_pdf_gcs_to_csv(
                gs_pdf_uri=gs_pdf_uri,
                dest_bucket=DEST_BUCKET,
                dest_prefix=dest_prefix,
                client=client,
                skip_if_exists=SKIP_IF_EXISTS,
                company=ticker,  
                years=YEARS,     
            )
            print(f"[{i}/{len(pdfs)}] OK -> {out_uri}")
            ok += 1

        except Exception as e:
            print(f"[{i}/{len(pdfs)}] FAILED: {gs_pdf_uri}\n  -> {type(e).__name__}: {e}")
            failed += 1

    print("\n==== SUMMARY ====")
    print(f"OK: {ok}")
    print(f"FAILED: {failed}")
    print("Done.")


if __name__ == "__main__":
    main()
