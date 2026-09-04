#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ipaddress
from pathlib import Path
import re
import sqlite3
from urllib.parse import urlsplit, urlunsplit

from pycluster.config import load_config
from pycluster.netutil import detected_public_ip_addresses, valid_global_ip
from pycluster.protocol import WirePcFrame, sanitize_pc92_private_ips


def _database_path(config_path: Path, runtime_root: Path) -> tuple[object, Path]:
    config = load_config(config_path)
    path = Path(str(config.store.sqlite_path)).expanduser()
    if not path.is_absolute():
        path = runtime_root / path
    return config, path


def _shown(value: str, family: str, privacy: str) -> str:
    if not value:
        return "<none>"
    return value if privacy == "unredacted" else f"<{family}-address-redacted>"


def _address_source(configured: str, valid: str, detected: str) -> str:
    if valid:
        return "configured"
    if configured:
        return "configured-invalid; detected fallback" if detected else "configured-invalid; no fallback"
    return "detected" if detected else "unavailable"


def _clean_dsn(raw: str, privacy: str) -> tuple[str, str]:
    text = str(raw or "").strip()
    if not text:
        return "<none>", "missing"
    try:
        parsed = urlsplit(text)
        host = parsed.hostname or ""
        port = parsed.port
        host_class = "hostname"
        if host.lower() == "localhost":
            host_class = "localhost"
        else:
            try:
                ip = ipaddress.ip_address(host.split("%", 1)[0])
                host_class = "global-address" if ip.is_global else "local-or-private-address"
            except ValueError:
                pass
        rendered_host = "[host-redacted]" if privacy == "redacted" else (f"[{host}]" if ":" in host else host)
        netloc = rendered_host + (f":{port}" if port else "")
        # Paths, userinfo, query strings, and fragments are unnecessary for
        # transport diagnosis and may contain implementation-specific secrets.
        return urlunsplit((parsed.scheme, netloc, "", "", "")), host_class
    except Exception:
        return "<dsn-redacted>", "unparseable"


def _local_address_token(token: str) -> bool:
    value = token.strip("[](),")
    if not value:
        return False
    if value.lower() == "localhost":
        return True
    try:
        return not ipaddress.ip_address(value).is_global
    except ValueError:
        return False


def _parsed_address(raw: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    value = raw.strip("[]() ")
    if "," in value and ":" not in value:
        value = value.replace(",", ":")
    try:
        return ipaddress.ip_address(value.split("%", 1)[0])
    except ValueError:
        return None


def _field_has_local_address(field: str, ipv4_re: re.Pattern[str]) -> bool:
    if "localhost" in field.lower() or any(_local_address_token(token) for token in ipv4_re.findall(field)):
        return True
    candidates = [field]
    if ":" in field:
        candidates.append(field.split(":", 1)[1])
    for candidate in candidates:
        parsed = _parsed_address(candidate)
        if parsed is not None and not parsed.is_global:
            return True
    return False


def _redact_local_field(field: str, ipv4_re: re.Pattern[str]) -> str:
    value = re.sub("localhost", "[localhost]", ipv4_re.sub("[ipv4-redacted]", field), flags=re.IGNORECASE)
    if ":" in field:
        prefix, suffix = field.split(":", 1)
        parsed = _parsed_address(suffix)
        if parsed is not None and not parsed.is_global:
            return f"{prefix}:[ip-address-redacted]"
    parsed = _parsed_address(field)
    if parsed is not None and not parsed.is_global:
        return "[ip-address-redacted]"
    return value


def _database_report(config: object, db_path: Path, privacy: str) -> None:
    print(f"store_path={db_path if privacy == 'unredacted' else '<path-redacted>'}")
    if not db_path.is_file():
        print("saved_peer_count=unavailable (database missing)")
        return
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5.0) as connection:
            quick_check = connection.execute("PRAGMA quick_check").fetchone()
            schema_version = connection.execute("PRAGMA user_version").fetchone()
            rows = connection.execute(
                "SELECT pref_key, pref_value FROM user_prefs "
                "WHERE call = ? AND pref_key LIKE 'peer.target.%' ORDER BY pref_key",
                (config.node.node_call.upper(),),
            ).fetchall()
            table_counts: dict[str, object] = {}
            for table in ("spots", "messages", "bulletins", "user_registry", "user_prefs", "py_nodes"):
                try:
                    table_counts[table] = connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                except sqlite3.DatabaseError:
                    table_counts[table] = "unavailable"
        print(f"database_quick_check={quick_check[0] if quick_check else 'unavailable'}")
        print(f"database_schema_version={schema_version[0] if schema_version else 'unavailable'}")
        print("database_table_counts=" + " ".join(f"{key}:{value}" for key, value in table_counts.items()))
        peers: dict[str, dict[str, str]] = {}
        for key, value in rows:
            rest = str(key)[len("peer.target."):]
            if "." not in rest:
                continue
            slug, field = rest.split(".", 1)
            peers.setdefault(slug, {})[field] = str(value or "")
        print(f"saved_peer_count={len(peers)}")
        for row in peers.values():
            dsn, host_class = _clean_dsn(row.get("dsn", ""), privacy)
            print(
                "peer "
                f"name={row.get('name', '<unknown>')} profile={row.get('profile', '<unknown>')} "
                f"reconnect={row.get('reconnect', '<unknown>')} endpoint={dsn} host_class={host_class} "
                f"password_saved={bool(row.get('password', ''))}"
            )
    except Exception as exc:
        print(f"peer_diagnostic_error={type(exc).__name__}: {exc}")


def _protocol_log_report(runtime_root: Path, privacy: str) -> None:
    proto_root = runtime_root / "logs" / "proto"
    logs = (
        sorted(proto_root.glob("*/*.log"), key=lambda item: item.stat().st_mtime, reverse=True)[:7]
        if proto_root.is_dir()
        else []
    )
    counts: dict[tuple[str, str], int] = {}
    findings: list[str] = []
    scanned = 0
    ipv4_re = re.compile(r"(?<![0-9.])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9.])")
    for log_path in logs:
        try:
            lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-2000:]
        except OSError:
            continue
        for line in lines:
            scanned += 1
            match = re.match(r"^(\S+)\s+(\S+)\s+(\S+)\s+((?:PC|PY)[0-9]{2})\^(.*)$", line)
            if not match:
                continue
            timestamp, peer, direction, frame_type, payload = match.groups()
            counts[(direction, frame_type)] = counts.get((direction, frame_type), 0) + 1
            if frame_type not in {"PC61", "PC92", "PC93"}:
                continue
            fields = payload.split("^")
            suspect_fields = [
                field
                for field in fields
                if _field_has_local_address(field, ipv4_re)
            ]
            if not suspect_fields or len(findings) >= 40:
                continue
            if privacy == "redacted":
                suspect_fields = [_redact_local_field(value, ipv4_re) for value in suspect_fields]
            origin = fields[0] if fields else "<unknown>"
            findings.append(
                f"{timestamp} peer={peer} direction={direction} frame={frame_type} "
                f"origin={origin} local_fields={suspect_fields}"
            )
    print(f"protocol_logs={len(logs)} scanned_lines={scanned}")
    for (direction, frame_type), count in sorted(counts.items()):
        if frame_type in {"PC18", "PC61", "PC92", "PC93", "PY00", "PY01"}:
            print(f"protocol_count direction={direction} frame={frame_type} count={count}")
    print(f"local_address_findings={len(findings)}")
    for finding in findings:
        print(finding)


def report(config_path: Path, runtime_root: Path, privacy: str) -> None:
    config, db_path = _database_path(config_path, runtime_root)
    configured_v4 = str(config.node.public_ip_address or "").strip()
    configured_v6 = str(config.node.public_ipv6_address or "").strip()
    valid_v4 = valid_global_ip(configured_v4, version=4)
    valid_v6 = valid_global_ip(configured_v6, version=6)
    detected = detected_public_ip_addresses()
    effective_v4 = valid_v4 or detected.get("ipv4", "")
    effective_v6 = valid_v6 or detected.get("ipv6", "")

    print(f"node_call={config.node.node_call}")
    print(f"configured_ipv4={_shown(configured_v4, 'ipv4', privacy)} valid={bool(valid_v4)}")
    print(f"configured_ipv6={_shown(configured_v6, 'ipv6', privacy)} valid={bool(valid_v6)}")
    print(f"detected_ipv4={_shown(detected.get('ipv4', ''), 'ipv4', privacy)}")
    print(f"detected_ipv6={_shown(detected.get('ipv6', ''), 'ipv6', privacy)}")
    print(
        f"effective_pc_ipv4={_shown(effective_v4, 'ipv4', privacy)} "
        f"source={_address_source(configured_v4, valid_v4, detected.get('ipv4', ''))}"
    )
    print(
        f"effective_pc_ipv6={_shown(effective_v6, 'ipv6', privacy)} "
        f"source={_address_source(configured_v6, valid_v6, detected.get('ipv6', ''))}"
    )
    sample = WirePcFrame("PC92", [config.node.node_call, "0", "A", "", "7TEST-1:localhost", "H99", ""])
    sanitized = sanitize_pc92_private_ips(sample, effective_v4, effective_v6)
    status = "passed" if "localhost" not in sanitized.payload_fields[4].lower() else "unavailable"
    print(f"pc92_localhost_substitution={status}")
    _database_report(config, db_path, privacy)
    _protocol_log_report(runtime_root, privacy)


def backup_database(config_path: Path, runtime_root: Path, destination: Path) -> None:
    _config, source = _database_path(config_path, runtime_root)
    destination = destination.expanduser().resolve()
    destination.unlink(missing_ok=True)
    if not source.is_file():
        raise SystemExit(f"database source is missing: {source}")
    try:
        with sqlite3.connect(f"file:{source}?mode=ro", uri=True, timeout=5.0) as source_db:
            with sqlite3.connect(destination, timeout=5.0) as destination_db:
                source_db.backup(destination_db)
                check = destination_db.execute("PRAGMA integrity_check").fetchone()
    except BaseException:
        destination.unlink(missing_ok=True)
        raise
    if not check or check[0] != "ok":
        destination.unlink(missing_ok=True)
        raise SystemExit("exported database failed integrity_check")
    destination.chmod(0o600)
    print(f"SQLite snapshot written: {destination}")
    print(f"Snapshot size: {destination.stat().st_size} bytes")


def main() -> None:
    parser = argparse.ArgumentParser(description="pyCluster support-bundle data helper")
    subparsers = parser.add_subparsers(dest="command", required=True)
    report_parser = subparsers.add_parser("report")
    report_parser.add_argument("--config", required=True, type=Path)
    report_parser.add_argument("--runtime-root", required=True, type=Path)
    report_parser.add_argument("--privacy", choices=("redacted", "unredacted"), required=True)
    backup_parser = subparsers.add_parser("backup-database")
    backup_parser.add_argument("--config", required=True, type=Path)
    backup_parser.add_argument("--runtime-root", required=True, type=Path)
    backup_parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "report":
        report(args.config.resolve(), args.runtime_root.resolve(), args.privacy)
    else:
        backup_database(args.config.resolve(), args.runtime_root.resolve(), args.output)


if __name__ == "__main__":
    main()
