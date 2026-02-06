from __future__ import annotations

import os
from typing import Optional, List

from google.cloud import storage

from collect_upload_gcs import (
    list_pdfs_in_gcs,
    download_gcs_pdf_to_temp,
    upload_markdown_to_gcs,
)
from gemini import pdf_to_markdown_gemini


def _gcs_exists(gs_uri: str, client: storage.Client) -> bool:
    assert gs_uri.startswith("gs://")
    _, rest = gs_uri.split("gs://", 1)
    bucket, blob_path = rest.split("/", 1)
    return client.bucket(bucket).blob(blob_path).exists(client=client)


def process_one_pdf_gcs_to_md(
    gs_pdf_uri: str,
    dest_bucket: str,
    dest_prefix: str,
    *,
    scale: float = 2.0,
    max_pages: Optional[int] = None,
    client: Optional[storage.Client] = None,
    skip_if_exists: bool = True,
) -> str:
    client = client or storage.Client()

    # output path
    pdf_name = gs_pdf_uri.rsplit("/", 1)[-1]
    out_name = os.path.splitext(pdf_name)[0] + ".md"
    out_blob_path = f"{dest_prefix.rstrip('/')}/{out_name}"
    out_uri = f"gs://{dest_bucket}/{out_blob_path}"

    # skip if already processed
    if skip_if_exists and _gcs_exists(out_uri, client):
        return out_uri

    local_pdf = download_gcs_pdf_to_temp(gs_pdf_uri, client)
    try:
        md = pdf_to_markdown_gemini(
            local_pdf,
            scale=scale,
            max_pages=max_pages,
            temperature=0.0,
        )
        return upload_markdown_to_gcs(dest_bucket, out_blob_path, md, client)
    finally:
        try:
            os.remove(local_pdf)
        except:
            pass


def process_prefix_gcs_to_md(
    src_bucket: str,
    src_prefix: str,
    dest_bucket: str,
    dest_prefix: str,
    *,
    scale: float = 2.0,
    max_pages: Optional[int] = None,
    skip_if_exists: bool = True,
) -> List[str]:
    
    client = storage.Client()
    pdfs = list_pdfs_in_gcs(src_bucket, src_prefix, client)

    out_uris: List[str] = []
    for gs_pdf_uri in pdfs:
        out_uris.append(
            process_one_pdf_gcs_to_md(
                gs_pdf_uri,
                dest_bucket,
                dest_prefix,
                scale=scale,
                max_pages=max_pages,
                client=client,            
                skip_if_exists=skip_if_exists,
            )
        )
    return out_uris
