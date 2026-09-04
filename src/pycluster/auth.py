from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import secrets


LOG = logging.getLogger(__name__)

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
        # Legacy plaintext record from a pre-hash database. Callers rehash on
        # a successful login, so this path should disappear as users sign in.
        LOG.warning("verifying a legacy plaintext password record; it will be rehashed on success")
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


async def hash_password_async(password: str, *, rounds: int = _PBKDF2_ROUNDS) -> str:
    """Hash off the event loop; PBKDF2 at these rounds takes hundreds of ms."""
    return await asyncio.to_thread(hash_password, password, rounds=rounds)


async def verify_password_async(password: str, stored: str | None) -> bool:
    """Verify off the event loop so one login cannot stall telnet and link I/O."""
    return await asyncio.to_thread(verify_password, password, stored)
