from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .config import AppConfig
from .models import Spot


def rbn_socket_address(config: AppConfig) -> str:
    db_path = Path(config.store.sqlite_path).expanduser().resolve()
    identity = str(db_path).encode("utf-8", errors="replace")
    digest = hashlib.sha256(identity).hexdigest()[:20]
    return str(db_path.parent / f".pycluster-rbn-{digest}.sock")


def encode_rbn_spot(spot: Spot) -> bytes:
    return json.dumps(
        {
            "freq_khz": float(spot.freq_khz),
            "dx_call": spot.dx_call,
            "epoch": int(spot.epoch),
            "info": spot.info,
            "spotter": spot.spotter,
            "source_node": spot.source_node,
            "raw": spot.raw,
        },
        separators=(",", ":"),
    ).encode("utf-8")


def decode_rbn_spot(payload: bytes) -> Spot:
    row = json.loads(payload.decode("utf-8"))
    return Spot(
        freq_khz=float(row["freq_khz"]),
        dx_call=str(row["dx_call"]),
        epoch=int(row["epoch"]),
        info=str(row.get("info") or ""),
        spotter=str(row["spotter"]),
        source_node=str(row.get("source_node") or "RBN"),
        raw=str(row.get("raw") or ""),
    )
