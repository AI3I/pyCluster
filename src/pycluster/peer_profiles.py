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
    if p in {"pycluster", "dxspider", "arcluster", "dxnet", "clx"}:
        return p
    return "pycluster"


def allowed_types_for_profile(profile: str) -> set[str]:
    p = normalize_profile(profile)
    return set(_ALLOWED_BY_PROFILE[p])


def profile_allows_pc(profile: str, pc_type: str) -> bool:
    allowed = allowed_types_for_profile(profile)
    if not allowed:
        return True
    return pc_type.upper() in allowed


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
    suffix_text = f" {(suffix or '').strip()}".rstrip()
    spot = (spotter or "")[:11]
    dx = (dx_call or "")[:12]
    info_width = max(0, 31 - len(suffix_text))
    inf = _normalize_dx_info(info)[:info_width]
    prefix = f"DX de {spot}:"
    return f"{prefix:<19}{freq_khz:8.1f}  {dx:<12}  {inf:<{info_width}} {when:>5}{suffix_text}"[:80]
