from __future__ import annotations

from dataclasses import dataclass

from .store import SpotStore


DEFAULT_SPOT_THROTTLE_MAX = 10
DEFAULT_SPOT_THROTTLE_WINDOW_SECONDS = 300
SPOT_THROTTLE_MAX_KEY = "spot_throttle.max_per_window"
SPOT_THROTTLE_WINDOW_KEY = "spot_throttle.window_seconds"
SPOT_THROTTLE_EXEMPT_KEY = "spot_throttle.exempt"


@dataclass(slots=True)
class SpotThrottlePolicy:
    call: str
    privilege: str
    exempt: bool
    enabled: bool
    max_per_window: int
    window_seconds: int
    recent_count: int = 0
    override_scope: str = ""


def _parse_int(raw: str | None) -> int | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _is_on_value(raw: str | None) -> bool:
    return str(raw or "").strip().lower() in {"1", "on", "yes", "true"}


async def _subject_privilege(store: SpotStore, call: str) -> str:
    target = call.upper()
    base = target.split("-", 1)[0]
    privilege = ""
    for candidate in (target, base):
        row = await store.get_user_registry(candidate)
        if row and not privilege:
            privilege = str(row["privilege"] or "").strip().lower()
        if not privilege:
            privilege = str(await store.get_user_pref(candidate, "privilege") or "").strip().lower()
    return privilege


async def load_spot_throttle_policy(store: SpotStore, node_call: str, call: str) -> SpotThrottlePolicy:
    target = call.upper()
    base = target.split("-", 1)[0]
    privilege = await _subject_privilege(store, target)
    default_max = _parse_int(await store.get_user_pref(node_call, SPOT_THROTTLE_MAX_KEY))
    if default_max is None:
        default_max = DEFAULT_SPOT_THROTTLE_MAX
    default_window = _parse_int(await store.get_user_pref(node_call, SPOT_THROTTLE_WINDOW_KEY))
    if default_window is None:
        default_window = DEFAULT_SPOT_THROTTLE_WINDOW_SECONDS

    override_scope = ""
    user_max: int | None = None
    user_window: int | None = None
    exempt: bool | None = None
    for candidate in (target, base):
        if exempt is None:
            raw_exempt = await store.get_user_pref(candidate, SPOT_THROTTLE_EXEMPT_KEY)
            if raw_exempt is not None and str(raw_exempt).strip():
                exempt = _is_on_value(raw_exempt)
                override_scope = candidate
        if user_max is None:
            parsed = _parse_int(await store.get_user_pref(candidate, SPOT_THROTTLE_MAX_KEY))
            if parsed is not None:
                user_max = parsed
                override_scope = candidate
        if user_window is None:
            parsed = _parse_int(await store.get_user_pref(candidate, SPOT_THROTTLE_WINDOW_KEY))
            if parsed is not None:
                user_window = parsed
                override_scope = candidate

    if exempt is None:
        exempt = privilege == "sysop"
    max_per_window = user_max if user_max is not None else default_max
    window_seconds = user_window if user_window is not None else default_window
    enabled = (not exempt) and max(0, int(max_per_window)) > 0 and max(0, int(window_seconds)) > 0
    return SpotThrottlePolicy(
        call=target,
        privilege=privilege,
        exempt=bool(exempt),
        enabled=enabled,
        max_per_window=max(0, int(max_per_window)),
        window_seconds=max(0, int(window_seconds)),
        override_scope=override_scope,
    )


async def check_spot_throttle(store: SpotStore, node_call: str, call: str, now_epoch: int) -> SpotThrottlePolicy:
    policy = await load_spot_throttle_policy(store, node_call, call)
    if not policy.enabled:
        return policy
    cutoff = int(now_epoch) - max(1, policy.window_seconds) + 1
    policy.recent_count = await store.count_recent_spots_by_spotter(policy.call, cutoff)
    return policy
