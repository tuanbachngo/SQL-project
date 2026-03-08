from __future__ import annotations

import os
import tempfile
from typing import Optional, List

from google.cloud import storage

from include.collect_upload_gcs import (
    list_pdfs_in_gcs,
    download_gcs_pdf_to_temp,
    upload_csv_to_gcs,
)
from include.data_select import extract_financial_data_pdf_to_csv


def _gcs_exists(gs_uri: str, client: storage.Client) -> bool:
    assert gs_uri.startswith("gs://")
    _, rest = gs_uri.split("gs://", 1)
    bucket, blob_path = rest.split("/", 1)
    return client.bucket(bucket).blob(blob_path).exists(client=client)


def process_one_pdf_gcs_to_csv(
    gs_pdf_uri: str,
    dest_bucket: str,
    dest_prefix: str,
    *,
    client: Optional[storage.Client] = None,
    skip_if_exists: bool = True,
    company: str = "",                
    years: Optional[List[int]] = None, 
) -> str:
    client = client or storage.Client()

    pdf_name = gs_pdf_uri.rsplit("/", 1)[-1]
    out_name = os.path.splitext(pdf_name)[0] + ".csv"
    out_blob_path = f"{dest_prefix.rstrip('/')}/{out_name}"
    out_uri = f"gs://{dest_bucket}/{out_blob_path}"

    if skip_if_exists and _gcs_exists(out_uri, client):
        return out_uri

    local_pdf = download_gcs_pdf_to_temp(gs_pdf_uri, client)

    fd, local_csv = tempfile.mkstemp(suffix=".csv")
    os.close(fd)

    try:
        extract_financial_data_pdf_to_csv(
            pdf_path=local_pdf,
            csv_path=local_csv,
            company=company,
            years=years,
        )

        with open(local_csv, "r", encoding="utf-8") as f:
            csv_text = f.read()

        return upload_csv_to_gcs(dest_bucket, out_blob_path, csv_text, client)

    finally:
        for p in (local_pdf, local_csv):
            try:
                os.remove(p)
            except:
                pass


def process_prefix_gcs_to_csv(
    src_bucket: str,
    src_prefix: str,
    dest_bucket: str,
    dest_prefix: str,
    *,
    skip_if_exists: bool = True,
    years: Optional[List[int]] = None,
) -> List[str]:
    client = storage.Client()
    pdfs = list_pdfs_in_gcs(src_bucket, src_prefix, client)

    out_uris: List[str] = []
    for gs_pdf_uri in pdfs:
        out_uris.append(
            process_one_pdf_gcs_to_csv(
                gs_pdf_uri=gs_pdf_uri,
                dest_bucket=dest_bucket,
                dest_prefix=dest_prefix,
                client=client,
                skip_if_exists=skip_if_exists,
                company="",  
                years=years,
            )
        )
    return out_uris