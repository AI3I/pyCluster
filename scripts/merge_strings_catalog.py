#!/usr/bin/env python3
"""Three-way merge bundled string defaults into an operator catalog."""

from __future__ import annotations

import argparse
import json
import math
import re
import tomllib
from pathlib import Path
from typing import Any


_MISSING = object()
_BARE_KEY = re.compile(r"^[A-Za-z0-9_-]+$")


def _merge(bundled: Any, runtime: Any, baseline: Any = _MISSING) -> Any:
    if isinstance(bundled, dict) and isinstance(runtime, dict):
        old = baseline if isinstance(baseline, dict) else {}
        merged: dict[str, Any] = {}
        for key in dict.fromkeys((*bundled, *runtime)):
            new_value = bundled.get(key, _MISSING)
            runtime_value = runtime.get(key, _MISSING)
            old_value = old.get(key, _MISSING)
            if runtime_value is _MISSING:
                if old_value is _MISSING:
                    merged[key] = new_value
                elif new_value is not _MISSING and old_value != new_value:
                    # An operator deletion remains deleted; a newly introduced key is installed.
                    continue
                continue
            if new_value is _MISSING:
                if old_value is _MISSING or runtime_value != old_value:
                    merged[key] = runtime_value
                continue
            merged[key] = _merge(new_value, runtime_value, old_value)
        return merged
    if baseline is not _MISSING and runtime == baseline:
        return bundled
    return runtime


def merge_catalogs(bundled: dict[str, Any], runtime: dict[str, Any], baseline: dict[str, Any] | None) -> dict[str, Any]:
    return _merge(bundled, runtime, baseline if baseline is not None else _MISSING)


def _key(value: str) -> str:
    return value if _BARE_KEY.fullmatch(value) else json.dumps(value, ensure_ascii=False)


def _value(value: Any) -> str:
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite floats are not supported in strings.toml")
        return repr(value)
    if isinstance(value, list) and not any(isinstance(item, dict) for item in value):
        return "[" + ", ".join(_value(item) for item in value) + "]"
    raise TypeError(f"unsupported TOML value: {type(value).__name__}")


def _emit_table(lines: list[str], path: tuple[str, ...], values: dict[str, Any], *, array: bool = False) -> None:
    if path:
        name = ".".join(_key(part) for part in path)
        lines.append(f"[[{name}]]" if array else f"[{name}]")
    scalars = [(key, value) for key, value in values.items() if not isinstance(value, dict) and not (isinstance(value, list) and any(isinstance(item, dict) for item in value))]
    for key, value in scalars:
        lines.append(f"{_key(key)} = {_value(value)}")
    if path or scalars:
        lines.append("")
    for key, value in values.items():
        if isinstance(value, dict):
            _emit_table(lines, (*path, key), value)
    for key, value in values.items():
        if isinstance(value, list) and any(isinstance(item, dict) for item in value):
            if not all(isinstance(item, dict) for item in value):
                raise TypeError(f"mixed arrays are not supported at {'.'.join((*path, key))}")
            for item in value:
                _emit_table(lines, (*path, key), item, array=True)


def dump_catalog(values: dict[str, Any]) -> str:
    lines: list[str] = []
    _emit_table(lines, (), values)
    return "\n".join(lines).rstrip() + "\n"


def _load(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        value = tomllib.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"catalog root must be a table: {path}")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundled", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    bundled = _load(args.bundled)
    runtime = _load(args.runtime)
    baseline = None
    if args.baseline and args.baseline.is_file():
        try:
            baseline = _load(args.baseline)
        except (OSError, tomllib.TOMLDecodeError, ValueError):
            baseline = None
    merged = merge_catalogs(bundled, runtime, baseline)
    text = args.runtime.read_text(encoding="utf-8") if merged == runtime else dump_catalog(merged)
    tomllib.loads(text)
    args.output.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
