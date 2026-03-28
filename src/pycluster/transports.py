from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
import socket
from typing import Any, Awaitable, Callable, Protocol
from urllib.parse import parse_qs, urlparse

from . import __version__

DXSPIDER_COMPAT_VERSION = "1.57"
DXSPIDER_COMPAT_BUILD = "46"


class LinkConnection(Protocol):
    name: str

    async def readline(self) -> str | None: ...

    async def send_line(self, line: str) -> None: ...

    async def close(self) -> None: ...


class LinkListener(Protocol):
    def listen_port(self) -> int | None: ...

    async def close(self) -> None: ...


def dxspider_compat_pc18(proto: str = "5457") -> str:
    software = (
        f"DXSpider Version: {DXSPIDER_COMPAT_VERSION} "
        f"Build: {DXSPIDER_COMPAT_BUILD} "
        f"Git: pyCluster/{__version__}"
    )
    return f"PC18^{software}^{proto}^"


@dataclass(slots=True)
class TransportSpec:
    scheme: str
    host: str | None = None
    port: int | None = None
    path: str | None = None
    params: dict[str, str] | None = None


# KISS framing constants
FEND = 0xC0
FESC = 0xDB
TFEND = 0xDC
TFESC = 0xDD


def kiss_escape(payload: bytes) -> bytes:
    out = bytearray()
    for b in payload:
        if b == FEND:
            out.extend([FESC, TFEND])
        elif b == FESC:
            out.extend([FESC, TFESC])
        else:
            out.append(b)
    return bytes(out)


def kiss_unescape(payload: bytes) -> bytes:
    out = bytearray()
    i = 0
    while i < len(payload):
        b = payload[i]
        if b == FESC and i + 1 < len(payload):
            nxt = payload[i + 1]
            if nxt == TFEND:
                out.append(FEND)
            elif nxt == TFESC:
                out.append(FESC)
            else:
                out.append(nxt)
            i += 2
            continue
        out.append(b)
        i += 1
    return bytes(out)


def kiss_encode_data_frame(payload: bytes, tnc_port: int = 0) -> bytes:
    cmd = ((tnc_port & 0x0F) << 4) | 0x00
    return bytes([FEND, cmd]) + kiss_escape(payload) + bytes([FEND])


def kiss_extract_data_payloads(stream: bytes) -> tuple[list[bytes], bytes]:
    """Extract KISS data payloads from a stream buffer.

    Returns (decoded_payloads, remaining_bytes).
    """
    payloads: list[bytes] = []
    start = stream.find(bytes([FEND]))
    if start < 0:
        return payloads, stream

    i = start + 1
    while i < len(stream):
        end = stream.find(bytes([FEND]), i)
        if end < 0:
            return payloads, stream[start:]

        frame = stream[i:end]
        if frame:
            cmd = frame[0]
            cmd_type = cmd & 0x0F
            if cmd_type == 0x00:  # data frame
                raw_payload = frame[1:]
                payloads.append(kiss_unescape(raw_payload))

        i = end + 1
        start = end

    return payloads, b""


def parse_transport_dsn(dsn: str) -> TransportSpec:
    u = urlparse(dsn)
    scheme = (u.scheme or "").lower()
    if not scheme:
        raise ValueError("transport dsn must include scheme, e.g. tcp://127.0.0.1:7300")

    q = {k: v[0] for k, v in parse_qs(u.query).items() if v}

    if scheme == "tcp":
        if not u.hostname or u.port is None:
            raise ValueError("tcp dsn must include host and port")
        return TransportSpec(scheme=scheme, host=u.hostname, port=u.port, params=q)

    if scheme in {"dxspider", "spidertelnet"}:
        if not u.hostname or u.port is None:
            raise ValueError("dxspider dsn must include host and port")
        return TransportSpec(scheme="dxspider", host=u.hostname, port=u.port, params=q)

    if scheme in {"kiss", "kiss_serial"}:
        path = u.path or ""
        if not path:
            raise ValueError("kiss dsn must include serial device path, e.g. kiss:///dev/ttyUSB0?baud=9600")
        return TransportSpec(scheme="kiss_serial", path=path, params=q)

    if scheme in {"ax25", "ax25_socket"}:
        host = u.hostname.upper() if u.hostname else None
        path = u.path[1:].upper() if u.path.startswith("/") and len(u.path) > 1 else None
        return TransportSpec(scheme="ax25_socket", host=host, path=path, params=q)

    raise ValueError(f"unsupported transport scheme: {scheme}")

async def _wait_closed_with_timeout(waiter, timeout: float = 1.0) -> None:
    try:
        await asyncio.wait_for(waiter, timeout=timeout)
    except (asyncio.TimeoutError, ConnectionError, OSError):
        return


@dataclass(slots=True)
class _TcpConnection:
    name: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    async def readline(self) -> str | None:
        raw = await self.reader.readline()
        if not raw:
            return None
        return raw.decode("utf-8", errors="replace").rstrip("\r\n")

    async def send_line(self, line: str) -> None:
        self.writer.write((line + "\n").encode("utf-8", errors="replace"))
        await self.writer.drain()

    async def close(self) -> None:
        self.writer.close()
        await _wait_closed_with_timeout(self.writer.wait_closed())


class _TcpListener:
    def __init__(self, server: asyncio.AbstractServer) -> None:
        self._server = server

    def listen_port(self) -> int | None:
        if not self._server.sockets:
            return None
        return int(self._server.sockets[0].getsockname()[1])

    async def close(self) -> None:
        self._server.close()
        await _wait_closed_with_timeout(self._server.wait_closed())


class _DxSpiderTelnetConnection:
    def __init__(self, name: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.name = name
        self._reader = reader
        self._writer = writer
        self._buf = bytearray()
        self._closed = False

    @staticmethod
    def _strip_telnet(data: bytes) -> bytes:
        out = bytearray()
        i = 0
        while i < len(data):
            b = data[i]
            if b != 255:
                out.append(b)
                i += 1
                continue
            if i + 1 >= len(data):
                break
            cmd = data[i + 1]
            if cmd == 255:
                out.append(255)
                i += 2
                continue
            if cmd in {251, 252, 253, 254}:
                i += 3
                continue
            if cmd == 250:  # SB ... IAC SE
                j = i + 2
                while j + 1 < len(data):
                    if data[j] == 255 and data[j + 1] == 240:
                        j += 2
                        break
                    j += 1
                i = j
                continue
            i += 2
        return bytes(out)

    async def _read_chunk(self) -> bytes:
        raw = await self._reader.read(1024)
        if not raw:
            return b""
        return self._strip_telnet(raw)

    @staticmethod
    def _has_pc_line(buf: bytes) -> bool:
        for raw in buf.splitlines():
            if raw.lstrip().startswith(b"PC"):
                return True
        return False

    @staticmethod
    def _complete_lines(buf: bytes) -> list[bytes]:
        out: list[bytes] = []
        start = 0
        while True:
            nl = buf.find(b"\n", start)
            if nl < 0:
                break
            out.append(buf[start:nl].rstrip(b"\r"))
            start = nl + 1
        return out

    @staticmethod
    def _reply_pc18(buf: bytes) -> str:
        proto = "5457"
        for raw in _DxSpiderTelnetConnection._complete_lines(buf):
            line = raw.decode("utf-8", errors="replace").strip()
            if not line.startswith("PC18^"):
                continue
            fields = [part for part in line.split("^") if part]
            if len(fields) >= 3 and fields[2].isdigit():
                proto = fields[2]
                break
        return dxspider_compat_pc18(proto)

    async def handshake(self, login: str, client: str, password: str = "", timeout: float = 10.0) -> None:
        deadline = asyncio.get_running_loop().time() + max(1.0, timeout)
        sent_login = False
        sent_password = False
        sent_client = False
        sent_pc18 = False
        sent_pc20 = False
        while asyncio.get_running_loop().time() < deadline:
            text = bytes(self._buf)
            complete_lines = self._complete_lines(text)
            if sent_login and any(raw.lstrip().startswith(b"PC") for raw in complete_lines):
                if any(raw.lstrip().startswith(b"PC18") for raw in complete_lines):
                    if not sent_pc18:
                        await self.send_line(self._reply_pc18(text))
                        sent_pc18 = True
                    if not sent_pc20:
                        await self.send_line("PC20^")
                        sent_pc20 = True
                break
            if not sent_login and b"login:" in text.lower():
                await self.send_line(login)
                sent_login = True
                self._buf.clear()
                continue
            if sent_login and not sent_password and b"password:" in text.lower():
                if not password:
                    raise RuntimeError("dxspider peer requested password but none was configured")
                await self.send_line(password)
                sent_password = True
                self._buf.clear()
                continue
            if sent_login and (b">" in text or (sent_password and b"echoing is currently" in text.lower())):
                await self.send_line(f"client {client} telnet")
                sent_client = True
                self._buf.clear()
                break
            chunk = await asyncio.wait_for(self._read_chunk(), timeout=max(0.1, deadline - asyncio.get_running_loop().time()))
            if not chunk:
                break
            self._buf.extend(chunk)
        if not sent_login:
            raise RuntimeError("dxspider peer did not present login prompt")
        if not sent_client and not self._has_pc_line(bytes(self._buf)):
            await self.send_line(f"client {client} telnet")

    async def readline(self) -> str | None:
        while True:
            nl = self._buf.find(b"\n")
            if nl >= 0:
                raw = bytes(self._buf[:nl])
                del self._buf[: nl + 1]
                line = raw.decode("utf-8", errors="replace").rstrip("\r")
                if not line:
                    continue
                if not line.startswith("PC"):
                    continue
                return line
            chunk = await self._read_chunk()
            if not chunk:
                if not self._buf:
                    return None
                tail = self._buf.decode("utf-8", errors="replace").rstrip("\r")
                self._buf.clear()
                if tail.startswith("PC"):
                    return tail
                return None
            self._buf.extend(chunk)

    async def send_line(self, line: str) -> None:
        self._writer.write((line + "\r\n").encode("utf-8", errors="replace"))
        await self._writer.drain()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._writer.close()
        await _wait_closed_with_timeout(self._writer.wait_closed())


class DxSpiderInboundConnection:
    def __init__(
        self,
        name: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        initial_lines: list[str] | None = None,
    ) -> None:
        self.name = name
        self._reader = reader
        self._writer = writer
        self._buf = bytearray()
        self._pending = deque(initial_lines or [])
        self._closed = False

    async def _read_chunk(self) -> bytes:
        raw = await self._reader.read(1024)
        if not raw:
            return b""
        return _DxSpiderTelnetConnection._strip_telnet(raw)

    async def readline(self) -> str | None:
        if self._pending:
            return self._pending.popleft()
        while True:
            nl = self._buf.find(b"\n")
            if nl >= 0:
                raw = bytes(self._buf[:nl])
                del self._buf[: nl + 1]
                line = raw.decode("utf-8", errors="replace").rstrip("\r")
                if not line:
                    continue
                if not line.startswith("PC"):
                    continue
                return line
            chunk = await self._read_chunk()
            if not chunk:
                if not self._buf:
                    return None
                tail = self._buf.decode("utf-8", errors="replace").rstrip("\r")
                self._buf.clear()
                if tail.startswith("PC"):
                    return tail
                return None
            self._buf.extend(chunk)

    async def send_line(self, line: str) -> None:
        self._writer.write((line + "\r\n").encode("utf-8", errors="replace"))
        await self._writer.drain()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._writer.close()
        await _wait_closed_with_timeout(self._writer.wait_closed())


class _SocketLineConnection:
    def __init__(self, name: str, sock: socket.socket) -> None:
        self.name = name
        self._sock = sock
        self._sock.setblocking(False)
        self._buf = bytearray()

    async def readline(self) -> str | None:
        loop = asyncio.get_running_loop()
        while True:
            nl = self._buf.find(b"\n")
            if nl >= 0:
                raw = bytes(self._buf[:nl])
                del self._buf[: nl + 1]
                return raw.decode("utf-8", errors="replace").rstrip("\r")

            data = await loop.sock_recv(self._sock, 4096)
            if not data:
                return None if not self._buf else self._drain_tail()
            self._buf.extend(data)

    def _drain_tail(self) -> str:
        raw = bytes(self._buf)
        self._buf.clear()
        return raw.decode("utf-8", errors="replace").rstrip("\r")

    async def send_line(self, line: str) -> None:
        loop = asyncio.get_running_loop()
        await loop.sock_sendall(self._sock, (line + "\n").encode("utf-8", errors="replace"))

    async def close(self) -> None:
        self._sock.close()


class _Ax25Listener:
    def __init__(self, sock: socket.socket, task: asyncio.Task[None]) -> None:
        self._sock = sock
        self._task = task

    def listen_port(self) -> int | None:
        return None

    async def close(self) -> None:
        self._task.cancel()
        try:
            await self._task
        except BaseException:
            pass
        self._sock.close()


class _KissSerialConnection:
    def __init__(self, name: str, ser: Any, tnc_port: int = 0) -> None:
        self.name = name
        self._ser = ser
        self._tnc_port = tnc_port
        self._closed = False
        self._line_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=2048)
        self._partial_line = bytearray()
        self._rx_buf = bytearray()
        self._reader_task = asyncio.create_task(self._reader_loop(), name=f"kiss-reader-{name}")

    async def _reader_loop(self) -> None:
        try:
            while not self._closed:
                chunk = await asyncio.to_thread(self._ser.read, 512)
                if not chunk:
                    await asyncio.sleep(0.01)
                    continue
                self._rx_buf.extend(chunk)

                payloads, remain = kiss_extract_data_payloads(bytes(self._rx_buf))
                self._rx_buf = bytearray(remain)

                for p in payloads:
                    self._partial_line.extend(p)
                    while True:
                        nl = self._partial_line.find(b"\n")
                        if nl < 0:
                            break
                        raw_line = bytes(self._partial_line[:nl])
                        del self._partial_line[: nl + 1]
                        line = raw_line.decode("utf-8", errors="replace").rstrip("\r")
                        try:
                            self._line_queue.put_nowait(line)
                        except asyncio.QueueFull:
                            _ = self._line_queue.get_nowait()
                            self._line_queue.put_nowait(line)
        except Exception:
            pass

    async def readline(self) -> str | None:
        if self._closed:
            return None
        try:
            return await asyncio.wait_for(self._line_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            if self._closed:
                return None
            return ""

    async def send_line(self, line: str) -> None:
        if self._closed:
            raise ConnectionError("kiss serial connection closed")
        payload = (line + "\n").encode("utf-8", errors="replace")
        frame = kiss_encode_data_frame(payload, tnc_port=self._tnc_port)
        await asyncio.to_thread(self._ser.write, frame)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except BaseException:
                pass
        await asyncio.to_thread(self._ser.close)


OnAccept = Callable[[LinkConnection], Awaitable[None]]


async def _tcp_listen(host: str, port: int, on_accept: OnAccept) -> LinkListener:
    async def _handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        name = f"in:{peer}"
        conn = _TcpConnection(name=name, reader=reader, writer=writer)
        await on_accept(conn)

    server = await asyncio.start_server(_handler, host=host, port=port)
    return _TcpListener(server)


async def _tcp_connect(name: str, host: str, port: int) -> LinkConnection:
    reader, writer = await asyncio.open_connection(host, port)
    return _TcpConnection(name=name, reader=reader, writer=writer)


async def _dxspider_connect(name: str, spec: TransportSpec) -> LinkConnection:
    assert spec.host is not None and spec.port is not None
    params = spec.params or {}
    login = (params.get("login") or "").strip().upper()
    client = (params.get("client") or "").strip().upper()
    password = params.get("password", "")
    timeout = float(params.get("timeout", "10"))
    if not login:
        raise ValueError("dxspider dsn requires ?login=CALL")
    if not client:
        raise ValueError("dxspider dsn requires ?client=PEERCALL")
    reader, writer = await asyncio.open_connection(spec.host, spec.port)
    conn = _DxSpiderTelnetConnection(name=name, reader=reader, writer=writer)
    try:
        await conn.handshake(login=login, client=client, password=password, timeout=timeout)
    except Exception:
        await conn.close()
        raise
    return conn


def _require_ax25() -> int:
    af = getattr(socket, "AF_AX25", None)
    if af is None:
        raise RuntimeError("ax25_socket requires Linux AF_AX25 socket support")
    return int(af)


def _ax25_address_variants(dest: str, source: str | None, via: list[str]) -> list[object]:
    variants: list[object] = []
    # Python's AF_AX25 sockaddr representation varies between environments; try common forms.
    variants.append((dest,))
    variants.append(dest)
    if source:
        variants.append((dest, source))
    if via:
        variants.append((dest, tuple(via)))
    if source and via:
        variants.append((dest, source, tuple(via)))
    return variants


def _ax25_bind_variants(source: str) -> list[object]:
    return [(source,), source]


async def _ax25_connect(name: str, spec: TransportSpec) -> LinkConnection:
    af = _require_ax25()
    params = spec.params or {}

    dest = (spec.host or params.get("dest") or "").upper()
    if not dest:
        raise ValueError("ax25 connect dsn needs destination callsign in host or ?dest=")

    source = (params.get("source") or params.get("bind") or "").upper() or None
    via = [v.strip().upper() for v in params.get("via", "").split(",") if v.strip()]

    sock = socket.socket(af, socket.SOCK_STREAM)
    sock.setblocking(False)

    if source:
        bound = False
        for b in _ax25_bind_variants(source):
            try:
                sock.bind(b)  # type: ignore[arg-type]
                bound = True
                break
            except OSError:
                continue
        if not bound:
            sock.close()
            raise RuntimeError(f"unable to bind AX.25 source callsign: {source}")

    loop = asyncio.get_running_loop()
    last_err: Exception | None = None
    for addr in _ax25_address_variants(dest, source, via):
        try:
            await loop.sock_connect(sock, addr)  # type: ignore[arg-type]
            return _SocketLineConnection(name=name, sock=sock)
        except Exception as exc:
            last_err = exc
            continue

    sock.close()
    raise RuntimeError(f"unable to connect AX.25 destination {dest}: {last_err}")


async def _ax25_listen(spec: TransportSpec, on_accept: OnAccept) -> LinkListener:
    af = _require_ax25()
    params = spec.params or {}

    bind_call = (spec.host or params.get("bind") or params.get("source") or "").upper()
    if not bind_call:
        raise ValueError("ax25 listen dsn needs bind callsign in host or ?bind=")

    backlog = int(params.get("backlog", "8"))

    lsock = socket.socket(af, socket.SOCK_STREAM)
    lsock.setblocking(False)

    bound = False
    for b in _ax25_bind_variants(bind_call):
        try:
            lsock.bind(b)  # type: ignore[arg-type]
            bound = True
            break
        except OSError:
            continue
    if not bound:
        lsock.close()
        raise RuntimeError(f"unable to bind AX.25 listen callsign: {bind_call}")

    lsock.listen(backlog)

    async def _accept_loop() -> None:
        loop = asyncio.get_running_loop()
        while True:
            csock, addr = await loop.sock_accept(lsock)
            csock.setblocking(False)
            conn = _SocketLineConnection(name=f"in:{addr}", sock=csock)
            await on_accept(conn)

    task = asyncio.create_task(_accept_loop(), name=f"ax25-listen-{bind_call}")
    return _Ax25Listener(lsock, task)


def supported_transport_matrix() -> dict[str, dict[str, object]]:
    ax25_available = hasattr(socket, "AF_AX25")
    return {
        "tcp": {"implemented": True, "notes": "Line-delimited PCxx frames over TCP"},
        "dxspider": {
            "implemented": True,
            "notes": "Outbound telnet login/client handshake for legacy DXSpider-compatible node links",
        },
        "kiss_serial": {
            "implemented": True,
            "notes": "KISS over serial/USB TNC (connect mode). Requires pyserial.",
        },
        "ax25_socket": {
            "implemented": True,
            "available_on_this_host": ax25_available,
            "notes": "Experimental Linux AF_AX25 socket transport (connect + listen)",
        },
    }


async def listen_from_dsn(dsn: str, on_accept: OnAccept) -> LinkListener:
    spec = parse_transport_dsn(dsn)
    if spec.scheme == "tcp":
        assert spec.host is not None and spec.port is not None
        return await _tcp_listen(spec.host, spec.port, on_accept)
    if spec.scheme == "kiss_serial":
        raise NotImplementedError("kiss_serial listener not implemented (serial endpoint is connect-only)")
    if spec.scheme == "ax25_socket":
        return await _ax25_listen(spec, on_accept)
    raise ValueError(f"unsupported transport: {spec.scheme}")


async def connect_from_dsn(name: str, dsn: str) -> LinkConnection:
    spec = parse_transport_dsn(dsn)
    if spec.scheme == "tcp":
        assert spec.host is not None and spec.port is not None
        return await _tcp_connect(name, spec.host, spec.port)

    if spec.scheme == "dxspider":
        return await _dxspider_connect(name, spec)

    if spec.scheme == "kiss_serial":
        try:
            import serial  # type: ignore
        except ImportError as exc:
            raise RuntimeError("kiss_serial requires pyserial. Install with: pip install pyserial") from exc

        assert spec.path is not None
        params = spec.params or {}
        baud = int(params.get("baud", "9600"))
        tnc_port = int(params.get("tnc_port", params.get("port", "0")))

        ser = serial.Serial(spec.path, baudrate=baud, timeout=0.5)
        return _KissSerialConnection(name=name, ser=ser, tnc_port=tnc_port)

    if spec.scheme == "ax25_socket":
        return await _ax25_connect(name, spec)

    raise ValueError(f"unsupported transport: {spec.scheme}")
