from __future__ import annotations

import re

from .store import SpotStore


EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
REG_STATES = {"pending", "verified", "locked"}


def has_valid_email(email: str) -> bool:
    return bool(EMAIL_RE.match(str(email or "").strip()))


def normalize_registration_state(raw: str) -> str:
    txt = str(raw or "").strip().lower()
    if txt in REG_STATES:
        return txt
    return "pending"


async def registration_state(store: SpotStore, call: str) -> tuple[str, int, int]:
    state_raw = await store.get_user_pref(call, "registration_state")
    verified_raw = await store.get_user_pref(call, "email_verified_epoch")
    grace_raw = await store.get_user_pref(call, "grace_logins_remaining")
    state = normalize_registration_state(str(state_raw or ""))
    try:
        verified_epoch = max(0, int(str(verified_raw or "0").strip() or "0"))
    except ValueError:
        verified_epoch = 0
    try:
        grace_remaining = max(0, int(str(grace_raw or "0").strip() or "0"))
    except ValueError:
        grace_remaining = 0
    if verified_epoch > 0:
        state = "verified"
    return state, verified_epoch, grace_remaining


async def ensure_grace_logins(store: SpotStore, call: str, *, now_epoch: int, default_count: int) -> int:
    _state, _verified, remaining = await registration_state(store, call)
    if remaining > 0:
        return remaining
    remaining = max(0, int(default_count or 0))
    await store.set_user_pref(call, "grace_logins_remaining", str(remaining), now_epoch)
    return remaining


async def mark_email_unverified(store: SpotStore, call: str, *, now_epoch: int, grace_logins: int) -> None:
    await store.delete_user_pref(call, "email_verified_epoch")
    await store.set_user_pref(call, "registration_state", "pending", now_epoch)
    await store.set_user_pref(call, "grace_logins_remaining", str(max(0, int(grace_logins or 0))), now_epoch)


async def mark_email_verified(store: SpotStore, call: str, *, now_epoch: int) -> None:
    await store.set_user_pref(call, "email_verified_epoch", str(int(now_epoch)), now_epoch)
    await store.set_user_pref(call, "registration_state", "verified", now_epoch)


async def consume_grace_login(store: SpotStore, call: str, *, now_epoch: int, default_count: int) -> int:
    _state, _verified, remaining = await registration_state(store, call)
    if remaining <= 0:
        remaining = max(0, int(default_count or 0))
    remaining = max(0, remaining - 1)
    await store.set_user_pref(call, "grace_logins_remaining", str(remaining), now_epoch)
    if remaining <= 0:
        await store.set_user_pref(call, "registration_state", "locked", now_epoch)
    return remaining
