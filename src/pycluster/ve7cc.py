from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import unicodedata


@dataclass(frozen=True, slots=True)
class CC11Location:
    dxcc_id: str = ""
    cq_zone: str = ""
    itu_zone: str = ""
    state: str = ""
    entity: str = ""
    grid: str = ""


def _field(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", errors="ignore").decode("ascii")
    text = text.replace("^", " ").replace("\r", " ").replace("\n", " ")
    return " ".join(text.split())


def format_cc11(
    *,
    freq_khz: float,
    dx_call: str,
    epoch: int,
    info: str,
    spotter: str,
    source_node: str,
    spotted: CC11Location = CC11Location(),
    spotting: CC11Location = CC11Location(),
) -> str:
    when = datetime.fromtimestamp(int(epoch), tz=timezone.utc)
    fields = (
        "CC11",
        f"{float(freq_khz):.1f}",
        dx_call,
        when.strftime("%-d-%b-%Y"),
        when.strftime("%H%MZ"),
        info,
        spotter,
        spotted.dxcc_id,
        spotting.dxcc_id,
        source_node,
        spotted.cq_zone,
        spotted.itu_zone,
        spotting.cq_zone,
        spotting.itu_zone,
        spotted.state,
        spotting.state,
        spotted.entity,
        spotting.entity,
        spotted.grid,
        spotting.grid,
    )
    return "^".join(_field(value) for value in fields)
