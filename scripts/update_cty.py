#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
import sys
import tempfile
import tomllib
import urllib.request
from pathlib import Path


DEFAULT_CTY_URL = "https://www.country-files.com/cty/cty.dat"
DEFAULT_WPXLOC_URL = "https://www.country-files.com/cty/wpxloc.raw"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


REPO_ROOT = _repo_root()
sys.path.insert(0, str(REPO_ROOT / "src"))

from pycluster import ctydat  # noqa: E402
from pycluster import wpxloc  # noqa: E402


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _load_config(config_path: Path) -> dict:
    with config_path.open("rb") as fh:
        return tomllib.load(fh)


def _resolve_data_path(config_path: Path, configured_path: str, default_relative: str) -> Path:
    raw = str(configured_path or "").strip() or default_relative
    candidate = Path(raw)
    if candidate.is_absolute():
        return candidate
    app_root = config_path.parent.parent
    return (app_root / candidate).resolve()


def _validate_cty(path: Path) -> tuple[int, int]:
    ctydat.load_cty(str(path))
    prefix_count = len(ctydat._prefix_map)  # type: ignore[attr-defined]
    exact_count = len(ctydat._exact_map)  # type: ignore[attr-defined]
    if prefix_count < 1000:
        raise RuntimeError(f"cty.dat validation failed: only {prefix_count} prefixes loaded")
    if ctydat.lookup("K1JT") is None:
        raise RuntimeError("cty.dat validation failed: K1JT lookup returned no entity")
    return prefix_count, exact_count


def _validate_wpxloc(path: Path) -> tuple[int, int]:
    wpxloc.load_wpxloc(str(path))
    prefix_count = len(wpxloc._prefix_map)  # type: ignore[attr-defined]
    exact_count = len(wpxloc._exact_map)  # type: ignore[attr-defined]
    if prefix_count < 1000:
        raise RuntimeError(f"wpxloc.raw validation failed: only {prefix_count} prefixes loaded")
    if wpxloc.lookup("K1JT") is None:
        raise RuntimeError("wpxloc.raw validation failed: K1JT lookup returned no entity")
    return prefix_count, exact_count


def _download(url: str, label: str, min_size: int = 65536) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "pyCluster-country-updater/1.0 (+https://www.pycluster.org)",
            "Accept": "text/plain,application/octet-stream;q=0.9,*/*;q=0.1",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = resp.read()
    if len(data) < min_size:
        raise RuntimeError(f"downloaded {label} is unexpectedly small: {len(data)} bytes")
    return data


def _write_atomic(target: Path, data: bytes, *, prefix: str, suffix: str, validator) -> tuple[int, int]:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=str(target.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.chmod(tmp_path, 0o644)
        counts = validator(tmp_path)
        if target.exists():
            backup = target.with_suffix(target.suffix + ".bak")
            backup.write_bytes(target.read_bytes())
            os.chmod(backup, 0o644)
        os.replace(tmp_path, target)
        return counts
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _refresh_file(*, label: str, target: Path, url: str, validator, prefix: str, suffix: str) -> tuple[str, str]:
    old_bytes = target.read_bytes() if target.exists() else b""
    new_bytes = _download(url, label)
    digest = _sha256(new_bytes)[:12]
    if old_bytes and _sha256(old_bytes) == _sha256(new_bytes):
        left, right = validator(target)
        return (
            "unchanged",
            f"{label} unchanged at {target} ({left=}, {right=}, sha256={digest})",
        )
    left, right = _write_atomic(target, new_bytes, prefix=prefix, suffix=suffix, validator=validator)
    return (
        "updated",
        f"{label} updated at {target} from {url} ({left=}, {right=}, sha256={digest})",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh pyCluster CTY.DAT and WPXLOC.RAW from Country Files.")
    parser.add_argument("--config", default="config/pycluster.toml", help="Path to pyCluster config file.")
    parser.add_argument("--cty-url", default=DEFAULT_CTY_URL, help="CTY.DAT source URL.")
    parser.add_argument("--wpxloc-url", default=DEFAULT_WPXLOC_URL, help="WPXLOC.RAW source URL.")
    parser.add_argument("--cty-only", action="store_true", help="Refresh only CTY.DAT.")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    cfg = _load_config(config_path)
    public_web = cfg.get("public_web", {})
    cty_target = _resolve_data_path(config_path, str(public_web.get("cty_dat_path", "")), "fixtures/live/dxspider/cty.dat")
    wpx_target = _resolve_data_path(config_path, str(public_web.get("wpxloc_raw_path", "")), str(cty_target.with_name("wpxloc.raw")))

    results: list[str] = []
    _state, message = _refresh_file(
        label="CTY.DAT",
        target=cty_target,
        url=args.cty_url,
        validator=_validate_cty,
        prefix=".cty.",
        suffix=".dat",
    )
    results.append(message)

    if not args.cty_only:
        _state, message = _refresh_file(
            label="WPXLOC.RAW",
            target=wpx_target,
            url=args.wpxloc_url,
            validator=_validate_wpxloc,
            prefix=".wpxloc.",
            suffix=".raw",
        )
        results.append(message)

    for line in results:
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
