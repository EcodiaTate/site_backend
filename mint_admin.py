# mint_admin.py
import os, time
from jose import jwt

secret = os.getenv("JWT_SECRET")
if not secret:
    raise SystemExit("JWT_SECRET not set")

now = int(time.time())
exp = now + 60*60*24*7  # 7 days
payload = {
  "sub": "tate@ecodia.au",
  "scope": "admin",
  "aud": "admin",
  "iat": now,
  "exp": exp,
}
print(jwt.encode(payload, secret, algorithm=os.getenv("JWT_ALGO","HS256")))
