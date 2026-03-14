from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class DxSpiderWwvRecord:
    sender: str
    epoch: int
    sfi: int
    a_index: int
    k_index: int
    storm_text: str
    source_node: str
    accuracy: int

    @property
    def body(self) -> str:
        return f"SFI={self.sfi} A={self.a_index} K={self.k_index} {self.storm_text}"


@dataclass(slots=True)
class DxSpiderWcyRecord:
    epoch: int
    sfi: int
    a_index: int
    k_index: int
    sunspots: int
    expk: int
    aurora: str
    xray: str
    storm: str
    sender: str
    source_node: str

    @property
    def body(self) -> str:
        return (
            f"SFI={self.sfi} A={self.a_index} K={self.k_index} "
            f"spots={self.sunspots} expk={self.expk} "
            f"aurora={self.aurora} xray={self.xray} storm={self.storm}"
        )


def parse_dxspider_wwv_record(line: str) -> DxSpiderWwvRecord:
    parts = line.rstrip("\n").split("^")
    if len(parts) != 8:
        raise ValueError("WWV record must have 8 caret-separated fields")
    return DxSpiderWwvRecord(
        sender=parts[0].strip().upper(),
        epoch=int(parts[1]),
        sfi=int(parts[2]),
        a_index=int(parts[3]),
        k_index=int(parts[4]),
        storm_text=parts[5].strip(),
        source_node=parts[6].strip().upper(),
        accuracy=int(parts[7]),
    )


def parse_dxspider_wcy_record(line: str) -> DxSpiderWcyRecord:
    parts = line.rstrip("\n").split("^")
    if len(parts) != 11:
        raise ValueError("WCY record must have 11 caret-separated fields")
    return DxSpiderWcyRecord(
        epoch=int(parts[0]),
        sfi=int(parts[1]),
        a_index=int(parts[2]),
        k_index=int(parts[3]),
        sunspots=int(parts[4]),
        expk=int(parts[5]),
        aurora=parts[6].strip(),
        xray=parts[7].strip(),
        storm=parts[8].strip(),
        sender=parts[9].strip().upper(),
        source_node=parts[10].strip().upper(),
    )
