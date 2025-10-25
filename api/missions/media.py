import io, os, uuid
from typing import Optional, Tuple, Dict
from PIL import Image, ExifTags
import imagehash

UPLOAD_ROOT = os.getenv("UPLOAD_ROOT", "/data/uploads")

def _ensure_dirs() -> None:
    os.makedirs(UPLOAD_ROOT, exist_ok=True)

def save_image_and_fingerprints(file_bytes: bytes, ext_hint: str = ".jpg") -> Tuple[str, Dict]:
    _ensure_dirs()
    img = Image.open(io.BytesIO(file_bytes)).convert("RGB")
    phash = str(imagehash.phash(img))
    meta: Dict = {"phash": phash, "exif_gps": None}

    exif = getattr(img, "_getexif", lambda: None)()
    if exif:
        ex = {ExifTags.TAGS.get(k, k): v for k, v in exif.items()}
        gps = ex.get("GPSInfo")
        if gps:
            def _conv(val):
                d = val[0][0]/val[0][1]; m = val[1][0]/val[1][1]; s = val[2][0]/val[2][1]
                return d + m/60 + s/3600
            lat = _conv(gps.get(2)) if gps.get(2) else None
            lon = _conv(gps.get(4)) if gps.get(4) else None
            if lat and gps.get(1) == "S": lat = -lat
            if lon and gps.get(3) == "W": lon = -lon
            meta["exif_gps"] = {"lat": lat, "lon": lon}

    file_id = f"{uuid.uuid4().hex}{ext_hint.lower()}"
    path = os.path.join(UPLOAD_ROOT, file_id)
    img.save(path, quality=90, optimize=True)
    return path, meta
