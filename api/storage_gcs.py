from __future__ import annotations
import os
from functools import lru_cache
from datetime import timedelta
from typing import Optional
from google.cloud import storage
from google.auth.exceptions import DefaultCredentialsError


def _env(name: str, alt: Optional[str] = None) -> Optional[str]:
    """Fetch env var with optional fallback name."""
    v = os.getenv(name)
    return v if v is not None else (os.getenv(alt) if alt else None)


@lru_cache(maxsize=1)
def _bucket_name() -> str:
    # Support either EXPORT_BUCKET or GCS_EXPORT_BUCKET
    name = _env("EXPORT_BUCKET") or _env("GCS_EXPORT_BUCKET")
    if not name:
        raise RuntimeError(
            "Missing EXPORT_BUCKET (or GCS_EXPORT_BUCKET) environment variable. "
            "Set it to your GCS bucket name, e.g. 'ecodia-exports'."
        )
    return name


@lru_cache(maxsize=1)
def _client() -> storage.Client:
    try:
        # project is optional; ADC will infer from env/metadata if not set
        project = _env("GOOGLE_CLOUD_PROJECT") or _env("GCLOUD_PROJECT")
        return storage.Client(project=project) if project else storage.Client()
    except DefaultCredentialsError as e:
        raise RuntimeError(
            "Google Cloud credentials not found. For local dev, set "
            "GOOGLE_APPLICATION_CREDENTIALS to a service account JSON key, "
            "or run within Cloud Run/GCE with Workload Identity."
        ) from e


def upload_bytes(object_key: str, data: bytes, content_type: str = "application/zip") -> None:
    bucket = _client().bucket(_bucket_name())
    blob = bucket.blob(object_key)
    blob.upload_from_string(data, content_type=content_type)


def signed_url(object_key: str, expires_seconds: int = 900) -> str:
    bucket = _client().bucket(_bucket_name())
    blob = bucket.blob(object_key)

    # google-cloud-storage accepts int seconds or datetime/timedelta for v4
    exp = timedelta(seconds=expires_seconds)
    return blob.generate_signed_url(version="v4", expiration=exp, method="GET")
