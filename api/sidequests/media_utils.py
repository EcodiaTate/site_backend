from __future__ import annotations
import io, uuid
from typing import Tuple, Dict
from PIL import Image, ExifTags
from site_backend.core.paths import UPLOAD_ROOT

def _conv_gps(val):
    d = val[0][0]/val[0][1]; m = val[1][0]/val[1][1]; s = val[2][0]/val[2][1]
    return d + m/60 + s/3600

def save_image_and_fingerprints(file_bytes: bytes, ext_hint: str = ".jpg") -> Tuple[str, Dict]:
    img = Image.open(io.BytesIO(file_bytes)).convert("RGB")

    try:
        import imagehash
        phash = str(imagehash.phash(img))
    except Exception:
        phash = None

    meta: Dict = {"phash": phash, "exif_gps": None}

    exif = getattr(img, "_getexif", lambda: None)()
    if exif:
        from PIL import ExifTags
        ex = {ExifTags.TAGS.get(k, k): v for k, v in exif.items()}
        gps = ex.get("GPSInfo")
        if gps:
            lat = _conv_gps(gps.get(2)) if gps.get(2) else None
            lon = _conv_gps(gps.get(4)) if gps.get(4) else None
            if lat and gps.get(1) == "S": lat = -lat
            if lon and gps.get(3) == "W": lon = -lon
            meta["exif_gps"] = {"lat": lat, "lon": lon}

    upload_id = f"{uuid.uuid4().hex}{ext_hint.lower()}"
    fs_path = UPLOAD_ROOT / upload_id
    img.save(fs_path, quality=90, optimize=True)

    meta["upload_id"] = upload_id
    meta["web_path"] = f"/uploads/{upload_id}"
    return str(fs_path), meta
