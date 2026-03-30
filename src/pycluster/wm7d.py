from __future__ import annotations

from dataclasses import dataclass
import html
import re
import urllib.parse
import urllib.request


class WM7DLookupError(RuntimeError):
    pass


@dataclass(slots=True)
class WM7DLookupResult:
    callsign: str
    license_class: str = ""
    name: str = ""
    address_lines: tuple[str, ...] = ()


class WM7DClient:
    base_url = "https://www.wm7d.net/callsign/"

    async def lookup(self, callsign: str) -> WM7DLookupResult | None:
        return self._lookup_sync(callsign.upper())

    def _lookup_sync(self, callsign: str) -> WM7DLookupResult | None:
        url = self.base_url + urllib.parse.quote(callsign)
        req = urllib.request.Request(url, headers={"User-Agent": "pyCluster/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        if "returned no data" in body.lower():
            return None

        header_match = re.search(
            r"<b>Call:</b>\s*([^<\s]+)\s*<b>Class:</b>\s*([^<\n]+)",
            body,
            re.IGNORECASE | re.DOTALL,
        )
        block_match = re.search(
            r'<font size="\+1">.*?</font><br><br>\s*<b>(.*?)</b>',
            body,
            re.IGNORECASE | re.DOTALL,
        )
        if not header_match or not block_match:
            raise WM7DLookupError("WM7D response format was not recognized.")

        raw_block = block_match.group(1)
        raw_block = re.sub(r"<br\s*/?>", "\n", raw_block, flags=re.IGNORECASE)
        raw_block = re.sub(r"<[^>]+>", "", raw_block)
        lines = [html.unescape(part).strip() for part in raw_block.splitlines()]
        lines = [line for line in lines if line]
        name = lines[0] if lines else ""
        address_lines = tuple(lines[1:]) if len(lines) > 1 else ()
        return WM7DLookupResult(
            callsign=header_match.group(1).strip().upper(),
            license_class=html.unescape(header_match.group(2)).strip(),
            name=name,
            address_lines=address_lines,
        )
