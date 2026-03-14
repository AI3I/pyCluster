from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
from typing import Awaitable, Callable

from .peer_profiles import allowed_types_for_profile, normalize_profile, profile_allows_pc
from .protocol import WirePcFrame, decode_typed, parse_wire_pc_frame, serialize_wire_pc_frame
from .transports import LinkConnection, LinkListener, connect_from_dsn, listen_from_dsn


LOG = logging.getLogger(__name__)


@dataclass(slots=True)
class LinkPeer:
    name: str
    conn: LinkConnection
    inbound: bool
    profile: str = "dxspider"
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


class NodeLinkEngine:
    """Lightweight node-link engine for controlled compatibility testing.

    Wire format is line-delimited `PCxx^field^field...` frames.
    """

    def __init__(self) -> None:
        self._listener: LinkListener | None = None
        self._peers: dict[str, LinkPeer] = {}
        self._lock = asyncio.Lock()
        self._frame_queue: asyncio.Queue[tuple[str, WirePcFrame, object | None]] = asyncio.Queue(maxsize=10000)
        self._trace_hook: Callable[[str, str, str], Awaitable[None]] | None = None
        self._reader_tasks: set[asyncio.Task[None]] = set()

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
        peer = LinkPeer(
            name=name,
            conn=conn,
            inbound=False,
            profile=normalize_profile(profile),
            connected_epoch=now,
        )
        async with self._lock:
            self._peers[name] = peer
        await self._trace(name, "connect", dsn)
        self._track_reader_task(asyncio.create_task(self._peer_reader(peer), name=f"node-link-reader-{name}"))

    async def accept_inbound(self, name: str, conn: LinkConnection, profile: str = "dxspider") -> None:
        now = int(datetime.now(timezone.utc).timestamp())
        peer = LinkPeer(
            name=name,
            conn=conn,
            inbound=True,
            profile=normalize_profile(profile),
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

    async def send(self, peer_name: str, frame: WirePcFrame) -> None:
        async with self._lock:
            peer = self._peers.get(peer_name)
        if peer is None:
            raise KeyError(f"unknown peer: {peer_name}")

        if not profile_allows_pc(peer.profile, frame.pc_type):
            peer.policy_dropped += 1
            peer.policy_by_reason["profile_tx_block"] += 1
            await self._trace(peer_name, "drop", f"profile_tx_block {serialize_wire_pc_frame(frame)}")
            return

        text = serialize_wire_pc_frame(frame)
        await self._trace(peer_name, "tx", text)
        await peer.conn.send_line(text)
        peer.sent_frames += 1
        peer.last_tx_epoch = int(datetime.now(timezone.utc).timestamp())
        peer.last_pc_type = frame.pc_type
        peer.tx_by_type[frame.pc_type] += 1

    async def peer_names(self) -> list[str]:
        async with self._lock:
            return sorted(self._peers.keys())

    async def broadcast(self, frame: WirePcFrame) -> int:
        names = await self.peer_names()
        sent = 0
        for name in names:
            try:
                await self.send(name, frame)
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
                frame = parse_wire_pc_frame(text)
                if frame is None:
                    peer.dropped_frames += 1
                    await self._trace(peer.name, "drop", "parse_error")
                    continue

                if not profile_allows_pc(peer.profile, frame.pc_type):
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
            if not peer.inbound:
                async with self._lock:
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
