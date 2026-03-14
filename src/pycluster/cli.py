from __future__ import annotations

import argparse
import asyncio
import logging

from .app import serve_core_forever, serve_forever, serve_public_forever
from .config import load_config
from .importer import import_spot_file
from .node_link import NodeLinkEngine
from .protocol import WirePcFrame
from .replay import format_stats, replay_frames
from .store import SpotStore
from .transports import supported_transport_matrix


def _logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="pyCluster DX-compatible server")
    p.add_argument("--config", default="./config/pycluster.toml", help="Path to config TOML")
    p.add_argument("--verbose", action="store_true")

    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("serve", help="Run all pyCluster services in one process")
    sub.add_parser("serve-core", help="Run telnet + admin + node-link services without public web")
    sub.add_parser("serve-public", help="Run public web UI/API service only")

    imp = sub.add_parser("import-spots", help="Import caret-separated spot file into sqlite")
    imp.add_argument("--file", required=True, help="Path to .dat spot file")

    rep = sub.add_parser("replay-frames", help="Replay and score PC frame parse compatibility")
    rep.add_argument("--input", required=True, help="Path to PC frame JSON or raw debug file")
    rep.add_argument("--report", default="./docs/replay-report.json", help="Output JSON report path")

    selft = sub.add_parser("link-selftest", help="Run local node-link loopback self-test")
    selft.add_argument("--host", default="127.0.0.1")
    selft.add_argument("--port", type=int, default=9730)

    sub.add_parser("link-adapters", help="Show supported node-link transport adapters")

    return p


async def _run_import(cfg_path: str, file_path: str) -> None:
    cfg = load_config(cfg_path)
    store = SpotStore(cfg.store.sqlite_path)
    try:
        imported, skipped = await import_spot_file(store, file_path)
        print(f"imported={imported} skipped={skipped}")
    finally:
        await store.close()


async def _run_link_selftest(host: str, port: int) -> None:
    engine = NodeLinkEngine()
    try:
        await engine.start_listener(host, port)
    except OSError as exc:
        print(f"link-selftest failed to bind {host}:{port}: {exc}")
        return
    try:
        await engine.connect("loop", host, port)
        await asyncio.sleep(0.05)

        await engine.send("loop", WirePcFrame("PC92", ["N0TEST-1", "0", "D", "", "5N0TEST-2", "H99", ""]))
        await engine.send("loop", WirePcFrame("PC61", ["14074.0", "K1ABC", "1-Mar-2026", "0000Z", "FT8", "N0CALL", "N0NODE-1", "127.0.0.1", "H1", "~"]))

        got = []
        for _ in range(2):
            item = await engine.recv(timeout=1.0)
            if item:
                got.append(item)

        stats = await engine.stats()
        print(f"received={len(got)}")
        print(f"peer_stats={stats}")
    finally:
        await engine.stop()


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    _logging(args.verbose)

    if args.cmd == "serve":
        cfg = load_config(args.config)
        asyncio.run(serve_forever(cfg))
        return

    if args.cmd == "serve-core":
        cfg = load_config(args.config)
        asyncio.run(serve_core_forever(cfg))
        return

    if args.cmd == "serve-public":
        cfg = load_config(args.config)
        asyncio.run(serve_public_forever(cfg))
        return

    if args.cmd == "import-spots":
        asyncio.run(_run_import(args.config, args.file))
        return

    if args.cmd == "replay-frames":
        stats = replay_frames(args.input, args.report)
        print(format_stats(stats))
        print(f"report={args.report}")
        return

    if args.cmd == "link-selftest":
        asyncio.run(_run_link_selftest(args.host, args.port))
        return

    if args.cmd == "link-adapters":
        matrix = supported_transport_matrix()
        for name, meta in matrix.items():
            status = "ready" if meta.get("implemented") else "planned"
            print(f"{name}: {status} - {meta.get('notes', '')}")
        return

    parser.error("unknown command")


if __name__ == "__main__":
    main()
