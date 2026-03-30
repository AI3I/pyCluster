from __future__ import annotations

import asyncio
from dataclasses import dataclass
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

from .config import QRZConfig
from .__init__ import __version__


class QRZLookupError(RuntimeError):
    pass


@dataclass(slots=True)
class QRZLookupResult:
    callsign: str
    fname: str = ""
    name: str = ""
    addr1: str = ""
    addr2: str = ""
    state: str = ""
    country: str = ""
    grid: str = ""
    county: str = ""
    lat: str = ""
    lon: str = ""
    dxcc: str = ""
    cqzone: str = ""
    ituzone: str = ""
    aliases: str = ""


class QRZClient:
    def __init__(self, config: QRZConfig) -> None:
        self._config = config
        self._session_key = ""
        self._lock = asyncio.Lock()

    def configured(self) -> bool:
        return bool(self._config.username.strip() and self._config.password.strip())

    async def lookup(self, callsign: str) -> QRZLookupResult | None:
        if not self.configured():
            raise QRZLookupError("QRZ lookup is not configured on this node.")
        async with self._lock:
            return await self._lookup_locked(callsign.upper())

    async def _lookup_locked(self, callsign: str) -> QRZLookupResult | None:
        if not self._session_key:
            self._session_key = await self._login_locked()
        payload = self._fetch_xml({"s": self._session_key, "callsign": callsign})
        session = payload.get("session", {})
        error = session.get("error", "")
        if error and any(token in error.lower() for token in ("session", "invalid", "timeout")):
            self._session_key = await self._login_locked()
            payload = self._fetch_xml({"s": self._session_key, "callsign": callsign})
            session = payload.get("session", {})
            error = session.get("error", "")
        if error:
            if "not found" in error.lower():
                return None
            raise QRZLookupError(error)
        data = payload.get("callsign", {})
        if not data:
            return None
        return QRZLookupResult(
            callsign=data.get("call", callsign),
            fname=data.get("fname", ""),
            name=data.get("name", ""),
            addr1=data.get("addr1", ""),
            addr2=data.get("addr2", ""),
            state=data.get("state", ""),
            country=data.get("country", ""),
            grid=data.get("grid", ""),
            county=data.get("county", ""),
            lat=data.get("lat", ""),
            lon=data.get("lon", ""),
            dxcc=data.get("dxcc", ""),
            cqzone=data.get("cqzone", ""),
            ituzone=data.get("ituzone", ""),
            aliases=data.get("aliases", ""),
        )

    async def _login_locked(self) -> str:
        payload = self._fetch_xml(
            {
                "username": self._config.username.strip(),
                "password": self._config.password,
                "agent": self._config.agent.strip() or f"pyCluster/{__version__}",
            }
        )
        session = payload.get("session", {})
        key = session.get("key", "")
        error = session.get("error", "")
        if error:
            raise QRZLookupError(error)
        if not key:
            raise QRZLookupError("QRZ login did not return a session key.")
        return key

    def _fetch_xml(self, params: dict[str, str]) -> dict[str, dict[str, str]]:
        query = urllib.parse.urlencode(params)
        req = urllib.request.Request(
            f"{self._config.api_url}?{query}",
            headers={"User-Agent": self._config.agent.strip() or f"pyCluster/{__version__}"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            xml_bytes = resp.read()
        root = ET.fromstring(xml_bytes)
        return _xml_payload(root)


def _xml_payload(root: ET.Element) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for child in root:
        section = _local_tag(child.tag)
        values: dict[str, str] = {}
        for node in child:
            values[_local_tag(node.tag)] = (node.text or "").strip()
        out[section] = values
    return out


def _local_tag(tag: str) -> str:
    if "}" in tag:
        tag = tag.rsplit("}", 1)[1]
    return tag.lower()
