from __future__ import annotations

import subprocess
import tomllib
import importlib.util
from pathlib import Path


_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "merge_strings_catalog.py"
_SPEC = importlib.util.spec_from_file_location("merge_strings_catalog", _SCRIPT)
assert _SPEC and _SPEC.loader
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
dump_catalog = _MODULE.dump_catalog
merge_catalogs = _MODULE.merge_catalogs


def test_first_merge_preserves_existing_values_and_adds_defaults() -> None:
    bundled = {"ui": {"title": "New title", "new": "Added"}, "extra": {"enabled": True}}
    runtime = {"ui": {"title": "Operator title"}}

    merged = merge_catalogs(bundled, runtime, None)

    assert merged == {"ui": {"title": "Operator title", "new": "Added"}, "extra": {"enabled": True}}


def test_three_way_merge_updates_defaults_and_preserves_customizations() -> None:
    baseline = {"ui": {"plain": "Old", "custom": "Old", "removed": "Old"}}
    bundled = {"ui": {"plain": "New", "custom": "New", "added": "Added"}}
    runtime = {"ui": {"plain": "Old", "custom": "Mine", "removed": "Old", "local": "Local"}}

    merged = merge_catalogs(bundled, runtime, baseline)

    assert merged == {"ui": {"plain": "New", "custom": "Mine", "added": "Added", "local": "Local"}}


def test_catalog_dump_round_trips_arrays_of_tables() -> None:
    value = {
        "public_web": {
            "taxonomy": {
                "mode_order": ["CW", "FT8"],
                "mode_rules": [{"pattern": "CW", "value": "CW", "button": "CW"}],
            }
        }
    }

    assert tomllib.loads(dump_catalog(value)) == value


def test_merge_script_writes_valid_catalog(tmp_path: Path) -> None:
    bundled = tmp_path / "bundled.toml"
    runtime = tmp_path / "runtime.toml"
    baseline = tmp_path / "baseline.toml"
    output = tmp_path / "merged.toml"
    bundled.write_text('[ui]\ntitle = "New"\nadded = "Added"\n', encoding="utf-8")
    runtime.write_text('[ui]\ntitle = "Mine"\n', encoding="utf-8")
    baseline.write_text('[ui]\ntitle = "Old"\n', encoding="utf-8")

    subprocess.run(
        [
            "python3",
            "scripts/merge_strings_catalog.py",
            "--bundled",
            str(bundled),
            "--runtime",
            str(runtime),
            "--baseline",
            str(baseline),
            "--output",
            str(output),
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
    )

    assert tomllib.loads(output.read_text(encoding="utf-8")) == {"ui": {"title": "Mine", "added": "Added"}}


def test_merge_script_keeps_unchanged_catalog_formatting(tmp_path: Path) -> None:
    bundled = tmp_path / "bundled.toml"
    runtime = tmp_path / "runtime.toml"
    output = tmp_path / "merged.toml"
    text = '# Operator comment\n\n[ui]\ntitle="Mine"\n'
    bundled.write_text(text, encoding="utf-8")
    runtime.write_text(text, encoding="utf-8")

    subprocess.run(
        [
            "python3",
            "scripts/merge_strings_catalog.py",
            "--bundled",
            str(bundled),
            "--runtime",
            str(runtime),
            "--output",
            str(output),
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
    )

    assert output.read_text(encoding="utf-8") == text
