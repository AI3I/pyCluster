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


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


REPO_ROOT = _repo_root()
sys.path.insert(0, str(REPO_ROOT / "src"))

from pycluster import ctydat  # noqa: E402


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _load_config(config_path: Path) -> dict:
    with config_path.open("rb") as fh:
        return tomllib.load(fh)


def _resolve_cty_path(config_path: Path, configured_path: str) -> Path:
    candidate = Path(configured_path)
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


def _download(url: str) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "pyCluster-cty-updater/1.0 (+https://www.pycluster.org)",
            "Accept": "text/plain,application/octet-stream;q=0.9,*/*;q=0.1",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = resp.read()
    if len(data) < 65536:
        raise RuntimeError(f"downloaded CTY.DAT is unexpectedly small: {len(data)} bytes")
    return data


def _write_atomic(target: Path, data: bytes) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".cty.", suffix=".dat", dir=str(target.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.chmod(tmp_path, 0o644)
        _validate_cty(tmp_path)
        if target.exists():
            backup = target.with_suffix(target.suffix + ".bak")
            backup.write_bytes(target.read_bytes())
            os.chmod(backup, 0o644)
        os.replace(tmp_path, target)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh pyCluster CTY.DAT from Country Files.")
    parser.add_argument("--config", default="config/pycluster.toml", help="Path to pyCluster config file.")
    parser.add_argument("--url", default=DEFAULT_CTY_URL, help="CTY.DAT source URL.")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    cfg = _load_config(config_path)
    cty_cfg = str(cfg.get("public_web", {}).get("cty_dat_path", "./fixtures/live/dxspider/cty.dat"))
    target = _resolve_cty_path(config_path, cty_cfg)

    old_bytes = target.read_bytes() if target.exists() else b""
    new_bytes = _download(args.url)
    if old_bytes and _sha256(old_bytes) == _sha256(new_bytes):
        prefixes, exacts = _validate_cty(target)
        print(
            f"CTY.DAT unchanged at {target} "
            f"(prefixes={prefixes}, exact={exacts}, sha256={_sha256(old_bytes)[:12]})"
        )
        return 0

    _write_atomic(target, new_bytes)
    prefixes, exacts = _validate_cty(target)
    print(
        f"CTY.DAT updated at {target} from {args.url} "
        f"(prefixes={prefixes}, exact={exacts}, sha256={_sha256(new_bytes)[:12]})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
