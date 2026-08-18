from __future__ import annotations

import asyncio
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import time
from typing import Awaitable, Callable

from .pathmeta import describe_transport_dsn
from .peer_profiles import allowed_types_for_profile, normalize_profile, profile_allows_frame
from .protocol import WirePcFrame, decode_typed, parse_wire_protocol_frame, sanitize_pc92_private_ips, serialize_wire_protocol_frame
from .transports import LinkConnection, LinkListener, connect_from_dsn, listen_from_dsn


LOG = logging.getLogger(__name__)


@dataclass(slots=True)
class LinkPeer:
    name: str
    conn: LinkConnection
    inbound: bool
    profile: str = "dxspider"
    transport: str = ""
    path_hint: str = ""
    connected_epoch: int = 0
    parsed_frames: int = 0
    sent_frames: int = 0
    dropped_frames: int = 0
    policy_dropped: int = 0
    policy_by_reason: Counter[str] = field(default_factory=Counter)
    last_rx_epoch: int | None = None
    last_tx_epoch: int | None = None
    last_pc_type: str | None = None
    rx_by_type: Counter[str] = field(default_factory=Counter)
    tx_by_type: Counter[str] = field(default_factory=Counter)
    py_rx_window: deque[tuple[float, int]] = field(default_factory=deque)
    py_tx_window: deque[tuple[float, int]] = field(default_factory=deque)
    py_rx_window_bytes: int = 0
    py_tx_window_bytes: int = 0


class NodeLinkEngine:
    """Lightweight node-link engine for controlled compatibility testing.

    Wire format is line-delimited `PCxx^field...` or pyCluster-only `PYxx^field...` frames.
    """

    def __init__(
        self,
        public_ip_address: str = "",
        public_ipv6_address: str = "",
        py_protocol_enabled: bool = False,
        max_py_frame_bytes: int = 2048,
        max_py_bytes_per_minute: int = 65536,
    ) -> None:
        self._listener: LinkListener | None = None
        self._peers: dict[str, LinkPeer] = {}
        self._lock = asyncio.Lock()
        self._frame_queue: asyncio.Queue[tuple[str, WirePcFrame, object | None]] = asyncio.Queue(maxsize=10000)
        self._trace_hook: Callable[[str, str, str], Awaitable[None]] | None = None
        self._reader_tasks: set[asyncio.Task[None]] = set()
        self.public_ip_address = str(public_ip_address or "").strip()
        self.public_ipv6_address = str(public_ipv6_address or "").strip()
        self.set_py_protocol_policy(py_protocol_enabled, max_py_frame_bytes, max_py_bytes_per_minute)

    def set_public_ip_address(self, public_ip_address: str, public_ipv6_address: str = "") -> None:
        self.public_ip_address = str(public_ip_address or "").strip()
        self.public_ipv6_address = str(public_ipv6_address or "").strip()

    def set_py_protocol_policy(self, enabled: bool, max_frame_bytes: int, max_bytes_per_minute: int) -> None:
        self.py_protocol_enabled = bool(enabled)
        self.max_py_frame_bytes = max(256, min(65536, int(max_frame_bytes)))
        self.max_py_bytes_per_minute = max(
            self.max_py_frame_bytes,
            min(10 * 1024 * 1024, int(max_bytes_per_minute)),
        )

    @staticmethod
    def _window_allows(peer: LinkPeer, direction: str, size: int, limit: int) -> bool:
        window = peer.py_rx_window if direction == "rx" else peer.py_tx_window
        total_attr = "py_rx_window_bytes" if direction == "rx" else "py_tx_window_bytes"
        total = int(getattr(peer, total_attr))
        now = time.monotonic()
        while window and now - window[0][0] >= 60.0:
            _epoch, expired_size = window.popleft()
            total -= expired_size
        if len(window) >= 10000 or total + size > limit:
            setattr(peer, total_attr, max(0, total))
            return False
        window.append((now, size))
        setattr(peer, total_attr, total + size)
        return True

    def set_trace_hook(self, hook: Callable[[str, str, str], Awaitable[None]] | None) -> None:
        self._trace_hook = hook

    async def start_listener(self, host: str, port: int) -> None:
        await self.start_listener_dsn(f"tcp://{host}:{port}")

    async def start_listener_dsn(self, dsn: str) -> None:
        self._listener = await listen_from_dsn(dsn, self._handle_inbound)
        LOG.info("node-link listener started dsn=%s", dsn)

    def listen_port(self) -> int | None:
        if not self._listener:
            return None
        return self._listener.listen_port()

    async def stop(self) -> None:
        if self._listener:
            await self._listener.close()
            self._listener = None

        async with self._lock:
            peers = list(self._peers.values())
            self._peers.clear()

        for p in peers:
            try:
                await asyncio.wait_for(p.conn.close(), timeout=1.0)
            except Exception:
                pass
        tasks = list(self._reader_tasks)
        self._reader_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=2.0)
            except asyncio.TimeoutError:
                LOG.warning("timed out waiting for node-link reader tasks to stop")

    async def connect(self, name: str, host: str, port: int) -> None:
        await self.connect_dsn(name, f"tcp://{host}:{port}")

    async def connect_dsn(self, name: str, dsn: str, profile: str = "dxspider") -> None:
        conn = await connect_from_dsn(name, dsn)
        now = int(datetime.now(timezone.utc).timestamp())
        transport, path_hint = describe_transport_dsn(dsn)
        peer = LinkPeer(
            name=name,
            conn=conn,
            inbound=False,
            profile=normalize_profile(profile),
            transport=transport or str(getattr(conn, "transport", "") or ""),
            path_hint=path_hint or str(getattr(conn, "path_hint", "") or ""),
            connected_epoch=now,
        )
        async with self._lock:
            self._peers[name] = peer
        trace_endpoint = " ".join(part for part in (peer.transport, peer.path_hint) if part).strip()
        await self._trace(name, "connect", trace_endpoint or "configured endpoint")
        self._track_reader_task(asyncio.create_task(self._peer_reader(peer), name=f"node-link-reader-{name}"))

    async def accept_inbound(self, name: str, conn: LinkConnection, profile: str = "dxspider") -> None:
        now = int(datetime.now(timezone.utc).timestamp())
        peer = LinkPeer(
            name=name,
            conn=conn,
            inbound=True,
            profile=normalize_profile(profile),
            transport=str(getattr(conn, "transport", "") or "tcp"),
            path_hint=str(getattr(conn, "path_hint", "") or ""),
            connected_epoch=now,
        )
        async with self._lock:
            old = self._peers.pop(name, None)
            self._peers[name] = peer
        if old is not None:
            try:
                await asyncio.wait_for(old.conn.close(), timeout=1.0)
            except Exception:
                pass
        await self._trace(name, "accept", "inbound")
        self._track_reader_task(asyncio.create_task(self._peer_reader(peer), name=f"node-link-reader-{name}"))

    def _track_reader_task(self, task: asyncio.Task[None]) -> None:
        self._reader_tasks.add(task)
        task.add_done_callback(self._reader_tasks.discard)

    async def set_peer_profile(self, peer_name: str, profile: str) -> bool:
        p = normalize_profile(profile)
        async with self._lock:
            peer = self._peers.get(peer_name)
            if not peer:
                return False
            peer.profile = p
            return True

    async def disconnect_peer(self, peer_name: str) -> bool:
        async with self._lock:
            peer = self._peers.pop(peer_name, None)
        if not peer:
            return False
        await self._trace(peer_name, "disconnect", "requested")
        try:
            await asyncio.wait_for(peer.conn.close(), timeout=1.0)
        except Exception:
            pass
        return True

    async def send(self, peer_name: str, frame: WirePcFrame) -> bool:
        async with self._lock:
            peer = self._peers.get(peer_name)
        if peer is None:
            raise KeyError(f"unknown peer: {peer_name}")

        frame = sanitize_pc92_private_ips(frame, self.public_ip_address, self.public_ipv6_address)

        is_py = frame.pc_type.upper().startswith("PY")
        text = serialize_wire_protocol_frame(frame)
        frame_size = len(text.encode("utf-8", errors="replace"))
        if is_py and not self.py_protocol_enabled:
            peer.policy_dropped += 1
            peer.policy_by_reason["py_disabled"] += 1
            await self._trace(peer_name, "drop", "py_disabled")
            return False
        if is_py and frame_size > self.max_py_frame_bytes:
            peer.policy_dropped += 1
            peer.policy_by_reason["py_frame_oversize"] += 1
            await self._trace(peer_name, "drop", "py_frame_oversize")
            return False
        if is_py and not self._window_allows(peer, "tx", frame_size, self.max_py_bytes_per_minute):
            peer.policy_dropped += 1
            peer.policy_by_reason["py_rate_limit"] += 1
            await self._trace(peer_name, "drop", "py_rate_limit")
            return False

        if not profile_allows_frame(peer.profile, frame.pc_type):
            peer.policy_dropped += 1
            peer.policy_by_reason["profile_tx_block"] += 1
            await self._trace(peer_name, "drop", f"profile_tx_block {serialize_wire_protocol_frame(frame)}")
            return False

        await self._trace(peer_name, "tx", text)
        try:
            await peer.conn.send_line(text)
        except (ConnectionError, OSError) as exc:
            async with self._lock:
                current = self._peers.get(peer_name)
                if current is peer:
                    self._peers.pop(peer_name, None)
            await self._trace(peer_name, "disconnect", f"tx_error {exc}")
            try:
                await asyncio.wait_for(peer.conn.close(), timeout=1.0)
            except Exception:
                pass
            raise
        peer.sent_frames += 1
        peer.last_tx_epoch = int(datetime.now(timezone.utc).timestamp())
        peer.last_pc_type = frame.pc_type
        peer.tx_by_type[frame.pc_type] += 1
        return True

    async def peer_names(self) -> list[str]:
        async with self._lock:
            return sorted(self._peers.keys())

    async def broadcast(self, frame: WirePcFrame) -> int:
        names = await self.peer_names()
        sent = 0
        for name in names:
            try:
                if await self.send(name, frame):
                    sent += 1
            except Exception:
                LOG.exception("node-link broadcast failed peer=%s pc=%s", name, frame.pc_type)
        return sent

    async def mark_policy_drop(self, peer_name: str, reason: str) -> None:
        r = (reason or "").strip().lower()
        if not r:
            r = "policy"
        async with self._lock:
            peer = self._peers.get(peer_name)
            if not peer:
                return
            peer.policy_dropped += 1
            peer.policy_by_reason[r] += 1

    async def clear_policy_drops(self, peer_filter: str | None = None) -> int:
        flt = (peer_filter or "").strip().lower()
        cleared = 0
        async with self._lock:
            for name, peer in self._peers.items():
                if flt and flt not in name.lower():
                    continue
                if peer.policy_dropped <= 0 and not peer.policy_by_reason:
                    continue
                peer.policy_dropped = 0
                peer.policy_by_reason.clear()
                cleared += 1
        return cleared

    async def recv(self, timeout: float | None = None) -> tuple[str, WirePcFrame, object | None] | None:
        try:
            if timeout is None:
                return await self._frame_queue.get()
            return await asyncio.wait_for(self._frame_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def stats(self) -> dict[str, dict[str, object]]:
        async with self._lock:
            peers = list(self._peers.values())
        out: dict[str, dict[str, object]] = {}
        for p in peers:
            out[p.name] = {
                "inbound": p.inbound,
                "profile": p.profile,
                "transport": p.transport,
                "path_hint": p.path_hint,
                "connected_epoch": p.connected_epoch,
                "parsed_frames": p.parsed_frames,
                "sent_frames": p.sent_frames,
                "dropped_frames": p.dropped_frames,
                "policy_dropped": p.policy_dropped,
                "policy_reasons": dict(p.policy_by_reason),
                "last_rx_epoch": p.last_rx_epoch,
                "last_tx_epoch": p.last_tx_epoch,
                "last_pc_type": p.last_pc_type,
                "allowed_types": sorted(allowed_types_for_profile(p.profile)),
                "rx_by_type": dict(p.rx_by_type),
                "tx_by_type": dict(p.tx_by_type),
            }
        return out

    async def _handle_inbound(self, conn: LinkConnection) -> None:
        now = int(datetime.now(timezone.utc).timestamp())
        peer = LinkPeer(name=conn.name, conn=conn, inbound=True, profile="dxspider", connected_epoch=now)

        async with self._lock:
            self._peers[peer.name] = peer
        await self._trace(peer.name, "accept", "inbound")

        try:
            await self._peer_reader(peer)
        finally:
            async with self._lock:
                self._peers.pop(peer.name, None)
            await self._trace(peer.name, "disconnect", "eof")
            try:
                await asyncio.wait_for(conn.close(), timeout=1.0)
            except Exception:
                pass

    async def _peer_reader(self, peer: LinkPeer) -> None:
        try:
            while True:
                text = await peer.conn.readline()
                if text is None:
                    break
                if text == "":
                    continue
                await self._trace(peer.name, "rx", text)
                frame = parse_wire_protocol_frame(text)
                if frame is None:
                    peer.dropped_frames += 1
                    await self._trace(peer.name, "drop", "parse_error")
                    continue

                if frame.pc_type.startswith("PY"):
                    frame_size = len(text.encode("utf-8", errors="replace"))
                    if not self.py_protocol_enabled:
                        peer.policy_dropped += 1
                        peer.policy_by_reason["py_disabled"] += 1
                        await self._trace(peer.name, "drop", "py_disabled")
                        continue
                    if frame_size > self.max_py_frame_bytes:
                        peer.policy_dropped += 1
                        peer.policy_by_reason["py_frame_oversize"] += 1
                        await self._trace(peer.name, "drop", "py_frame_oversize")
                        continue
                    if not self._window_allows(peer, "rx", frame_size, self.max_py_bytes_per_minute):
                        peer.policy_dropped += 1
                        peer.policy_by_reason["py_rate_limit"] += 1
                        await self._trace(peer.name, "drop", "py_rate_limit")
                        continue

                if not profile_allows_frame(peer.profile, frame.pc_type):
                    peer.policy_dropped += 1
                    peer.policy_by_reason["profile_rx_block"] += 1
                    await self._trace(peer.name, "drop", f"profile_rx_block {text}")
                    continue

                peer.parsed_frames += 1
                peer.last_rx_epoch = int(datetime.now(timezone.utc).timestamp())
                peer.last_pc_type = frame.pc_type
                peer.rx_by_type[frame.pc_type] += 1

                # Attempt typed decode for known families; None for others.
                typed = decode_typed_from_wire(frame)

                try:
                    self._frame_queue.put_nowait((peer.name, frame, typed))
                except asyncio.QueueFull:
                    peer.dropped_frames += 1
        except Exception:
            LOG.exception("node-link peer reader failed: %s", peer.name)
        finally:
            async with self._lock:
                current = self._peers.get(peer.name)
                if current is peer:
                    self._peers.pop(peer.name, None)
            try:
                await asyncio.wait_for(peer.conn.close(), timeout=1.0)
            except Exception:
                pass

    async def _trace(self, peer_name: str, direction: str, text: str) -> None:
        if not self._trace_hook:
            return
        try:
            await self._trace_hook(peer_name, direction, text)
        except Exception:
            LOG.exception("node-link trace hook failed peer=%s dir=%s", peer_name, direction)


def decode_typed_from_wire(frame: WirePcFrame) -> object | None:
    pseudo = type("Pseudo", (), {"pc_type": frame.pc_type, "payload_fields": frame.payload_fields})
    return decode_typed(pseudo)  # type: ignore[arg-type]
