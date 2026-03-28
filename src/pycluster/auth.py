from __future__ import annotations

import hashlib
import hmac
import secrets


_PBKDF2_NAME = "pbkdf2_sha256"
_PBKDF2_ROUNDS = 600_000


def is_password_hash(value: str | None) -> bool:
    raw = str(value or "").strip()
    return raw.startswith(f"{_PBKDF2_NAME}$")


def hash_password(password: str, *, rounds: int = _PBKDF2_ROUNDS) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("ascii"), rounds)
    return f"{_PBKDF2_NAME}${rounds}${salt}${digest.hex()}"


def verify_password(password: str, stored: str | None) -> bool:
    raw = str(stored or "").strip()
    if not raw:
        return False
    if not is_password_hash(raw):
        return hmac.compare_digest(password, raw)
    try:
        name, rounds_s, salt, digest_hex = raw.split("$", 3)
        if name != _PBKDF2_NAME:
            return False
        rounds = int(rounds_s)
    except Exception:
        return False
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("ascii"), rounds)
    return hmac.compare_digest(digest.hex(), digest_hex)
