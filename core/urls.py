# site_backend/core/urls.py
import os
from typing import Optional

PUBLIC_API_ORIGIN = os.getenv("PUBLIC_API_ORIGIN") or os.getenv("NEXT_PUBLIC_API_URL") or ""

def abs_media(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if not PUBLIC_API_ORIGIN:
        # Fallback: leave relative (dev can still work if FE is reverse-proxying)
        return url
    return f"{PUBLIC_API_ORIGIN.rstrip('/')}{url}"
