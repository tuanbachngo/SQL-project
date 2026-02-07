import tempfile
import os
from typing import List
from google.cloud import storage

def list_pdfs_in_gcs(bucket: str, prefix: str, client: storage.Client) -> List[str]:
    prefix = prefix.lstrip("/")
    uris = []
    for blob in client.list_blobs(bucket, prefix=prefix):
        if blob.name.endswith("/"):
            continue
        if blob.name.lower().endswith(".pdf"):
            uris.append(f"gs://{bucket}/{blob.name}")
    return uris

def download_gcs_pdf_to_temp(gs_uri: str, client: storage.Client) -> str:
    assert gs_uri.startswith("gs://")
    _, rest = gs_uri.split("gs://", 1)
    bucket_name, blob_path = rest.split("/", 1)

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    fd, tmp_path = tempfile.mkstemp(suffix=".pdf")
    os.close(fd) 
    blob.download_to_filename(tmp_path)
    return tmp_path

def upload_markdown_to_gcs(bucket: str, blob_path: str, markdown: str, client: storage.Client) -> str:
    blob = client.bucket(bucket).blob(blob_path)
    blob.upload_from_string(markdown.encode("utf-8"), content_type="text/markdown; charset=utf-8")
    return f"gs://{bucket}/{blob_path}" 

def upload_csv_to_gcs(bucket: str, blob_path: str, csv_text: str, client: storage.Client) -> str:
    blob = client.bucket(bucket).blob(blob_path)
    blob.upload_from_string(csv_text.encode("utf-8"), content_type="text/csv; charset=utf-8")
    return f"gs://{bucket}/{blob_path}"