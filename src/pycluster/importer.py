from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from .models import Spot, is_valid_call, normalize_call, parse_spot_record
from .protocol import Pc61Message, decode_typed, parse_debug_pc_frame, parse_wire_pc_frame
from .store import SpotStore


def _pc61_epoch(msg: Pc61Message, fallback_epoch: int | None = None) -> int:
    if fallback_epoch is not None and fallback_epoch > 0:
        return fallback_epoch
    date_token = (msg.date_token or "").strip()
    time_token = (msg.time_token or "").strip().upper()
    if date_token and time_token:
        try:
            dt = datetime.strptime(f"{date_token} {time_token}", "%d-%b-%Y %H%MZ")
            return int(dt.replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            pass
    return int(datetime.now(timezone.utc).timestamp())


def _pc61_to_spot(msg: Pc61Message, fallback_epoch: int | None = None, fallback_source: str = "") -> Spot | None:
    dx_call = normalize_call(msg.dx_call)
    spotter = normalize_call(msg.spotter)
    if not is_valid_call(dx_call) or not is_valid_call(spotter):
        return None
    try:
        freq_khz = float(msg.freq_khz)
    except ValueError:
        return None
    source_node = normalize_call(msg.source_node) if msg.source_node else normalize_call(fallback_source or "NODE")
    epoch = _pc61_epoch(msg, fallback_epoch)
    raw = "^".join(
        [
            f"{freq_khz:.1f}",
            dx_call,
            str(epoch),
            msg.info,
            spotter,
            "226",
            "226",
            source_node,
        ]
    )
    return Spot(
        freq_khz=freq_khz,
        dx_call=dx_call,
        epoch=epoch,
        info=msg.info,
        spotter=spotter,
        source_node=source_node,
        raw=raw,
    )


def _parse_import_line(line: str) -> Spot | None:
    text = line.strip()
    if not text:
        return None

    # Native pyCluster import format.
    try:
        return parse_spot_record(text)
    except Exception:
        pass

    # Wire protocol format: PC61^...
    wire = parse_wire_pc_frame(text)
    if wire and wire.pc_type == "PC61":
        msg = Pc61Message.from_fields(wire.payload_fields)
        return _pc61_to_spot(msg)

    # Debug log format: epoch^<- I LINK PC61^...
    dbg = parse_debug_pc_frame(text)
    if dbg and dbg.pc_type == "PC61":
        typed = decode_typed(dbg)
        msg = typed if isinstance(typed, Pc61Message) else Pc61Message.from_fields(dbg.payload_fields)
        return _pc61_to_spot(msg, fallback_epoch=dbg.epoch, fallback_source=dbg.link)

    return None


async def import_spot_file(store: SpotStore, file_path: str | Path) -> tuple[int, int]:
    src = Path(file_path)
    imported = 0
    skipped = 0
    batch = []

    for line in src.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        spot = _parse_import_line(line)
        if spot:
            batch.append(spot)
            imported += 1
        else:
            skipped += 1

        if len(batch) >= 1000:
            await store.add_spots(batch)
            batch.clear()

    if batch:
        await store.add_spots(batch)

    return imported, skipped
