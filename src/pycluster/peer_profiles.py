from __future__ import annotations

from typing import Literal


PeerProfile = Literal["pycluster", "dxspider", "arcluster", "dxnet", "clx"]


_ALLOWED_BY_PROFILE: dict[str, set[str]] = {
    "pycluster": set(),  # empty means allow all
    "dxspider": set(),  # empty means allow all
    "arcluster": {"PC61", "PC92", "PC93", "PC11", "PC24"},
    "dxnet": {"PC61", "PC92", "PC93"},
    "clx": {"PC61", "PC92", "PC93", "PC50"},
}


def normalize_profile(profile: str) -> str:
    p = (profile or "").strip().lower()
    if p == "spider":
        return "dxspider"
    if p in {"pycluster", "dxspider", "arcluster", "dxnet", "clx"}:
        return p
    return "pycluster"


def allowed_types_for_profile(profile: str) -> set[str]:
    p = normalize_profile(profile)
    return set(_ALLOWED_BY_PROFILE[p])


def profile_allows_frame(profile: str, frame_type: str) -> bool:
    if frame_type.upper().startswith("PY"):
        return normalize_profile(profile) == "pycluster"
    allowed = allowed_types_for_profile(profile)
    if not allowed:
        return True
    return frame_type.upper() in allowed


def profile_allows_pc(profile: str, pc_type: str) -> bool:
    return profile_allows_frame(profile, pc_type)


def _normalize_dx_info(info: str) -> str:
    text = (info or "").replace("\u00a0", " ")
    text = text.replace("ï¿½", " ")
    text = text.replace("\ufffd", " ")
    return " ".join(text.split())


def format_dx_line_for_profile(profile: str, freq_khz: float, dx_call: str, when: str, info: str, spotter: str) -> str:
    p = normalize_profile(profile)
    inf = _normalize_dx_info(info)[:28]

    if p == "arcluster":
        return f"{freq_khz:8.1f}  {dx_call:<12} {when}  {inf:<28} de {spotter}"
    if p == "dxnet":
        return f"{freq_khz:8.1f}  {dx_call:<12} {when}  {inf:<28} [{spotter}]"
    if p == "clx":
        return f"{freq_khz:8.1f}  {dx_call:<12} {when}  {inf:<28} by {spotter}"

    # DXSpider default
    return f"{freq_khz:8.1f}  {dx_call:<12} {when}  {inf:<28} <{spotter}>"


def format_live_dx_line_for_profile(
    profile: str,
    freq_khz: float,
    dx_call: str,
    when: str,
    info: str,
    spotter: str,
    suffix: str = "",
) -> str:
    _ = normalize_profile(profile)
    spot = (spotter or "")[:11]
    dx = (dx_call or "")[:12]
    prefix = f"DX de {spot}:"
    head = f"{prefix:<19}{freq_khz:8.1f}  {dx:<12}  "
    max_suffix = max(0, 79 - len(head) - 6)
    suffix_value = (suffix or "").strip()[:max_suffix]
    suffix_text = f" {suffix_value}" if suffix_value else ""
    tail = f" {when:>5}{suffix_text}"
    info_width = max(0, 79 - len(head) - len(tail))
    inf = _normalize_dx_info(info)[:info_width]
    return f"{head}{inf:<{info_width}}{tail}"
