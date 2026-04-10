from __future__ import annotations

from pathlib import Path
import logging
import tomllib


LOG = logging.getLogger(__name__)


class StringCatalog:
    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path else None
        self._mtime_ns: int | None = None
        self._data: dict[str, object] = {}

    def _reload_if_needed(self) -> None:
        if not self.path:
            return
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            self._mtime_ns = None
            self._data = {}
            return
        except OSError as exc:
            LOG.warning("string catalog stat failed for %s: %s", self.path, exc)
            return
        if self._mtime_ns == stat.st_mtime_ns:
            return
        try:
            raw = tomllib.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:
            LOG.warning("string catalog load failed for %s: %s", self.path, exc)
            return
        self._mtime_ns = stat.st_mtime_ns
        self._data = raw if isinstance(raw, dict) else {}

    def get(self, key: str, default: str = "") -> str:
        self._reload_if_needed()
        node: object = self._data
        for part in key.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return str(node) if isinstance(node, str) else default

    def data(self, key: str, default: object = None) -> object:
        self._reload_if_needed()
        node: object = self._data
        for part in key.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    def render(self, catalog_key: str, default: str = "", **values: object) -> str:
        template = self.get(catalog_key, default)

        class _SafeMap(dict[str, object]):
            def __missing__(self, missing_key: str) -> str:
                return "{" + missing_key + "}"

        return template.format_map(_SafeMap(values))
