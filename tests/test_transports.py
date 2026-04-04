from pycluster.transports import (
    connect_from_dsn,
    kiss_encode_data_frame,
    kiss_extract_data_payloads,
    parse_transport_dsn,
    supported_transport_matrix,
)
import asyncio
import pytest


def test_parse_tcp_dsn() -> None:
    spec = parse_transport_dsn("tcp://127.0.0.1:7300")
    assert spec.scheme == "tcp"
    assert spec.host == "127.0.0.1"
    assert spec.port == 7300


def test_parse_kiss_dsn() -> None:
    spec = parse_transport_dsn("kiss:///dev/ttyUSB0?baud=9600")
    assert spec.scheme == "kiss_serial"
    assert spec.path == "/dev/ttyUSB0"
    assert spec.params is not None
    assert spec.params.get("baud") == "9600"


def test_parse_dxspider_dsn() -> None:
    spec = parse_transport_dsn("dxspider://127.0.0.1:7300?login=AI3I-16&client=AI3I-15")
    assert spec.scheme == "dxspider"
    assert spec.host == "127.0.0.1"
    assert spec.port == 7300
    assert spec.params is not None
    assert spec.params.get("login") == "AI3I-16"
    assert spec.params.get("client") == "AI3I-15"


def test_parse_ax25_dsn() -> None:
    spec = parse_transport_dsn("ax25://N0DEST?source=N0SRC&via=RLY1,RLY2")
    assert spec.scheme == "ax25_socket"
    assert spec.host == "N0DEST"
    assert spec.params is not None
    assert spec.params.get("source") == "N0SRC"
    assert spec.params.get("via") == "RLY1,RLY2"


def test_supported_matrix() -> None:
    m = supported_transport_matrix()
    assert m["tcp"]["implemented"] is True
    assert m["dxspider"]["implemented"] is True
    assert m["kiss_serial"]["implemented"] is True
    assert m["ax25_socket"]["implemented"] is True


def test_kiss_encode_decode_roundtrip() -> None:
    payload = b"PC61^14074.0^K1ABC^1-Mar-2026^0000Z^FT8^N0CALL\\n"
    frame = kiss_encode_data_frame(payload, tnc_port=0)
    decoded, remain = kiss_extract_data_payloads(frame)
    assert remain == b""
    assert decoded == [payload]


def test_kiss_multiple_frames_in_stream() -> None:
    p1 = b"PC92^N0NODE-1^0^D^^5N0NODE-2\\n"
    p2 = b"PC93^N0NODE-1^0^*^N0NODE-1^*^hello\\n"
    stream = kiss_encode_data_frame(p1) + kiss_encode_data_frame(p2)
    decoded, remain = kiss_extract_data_payloads(stream)
    assert remain == b""
    assert decoded == [p1, p2]


def test_dxspider_connect_handshake_and_pc_readline() -> None:
    async def run() -> None:
        seen: list[str] = []

        async def _handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            writer.write(b"login: ")
            await writer.drain()
            login = (await reader.readline()).decode("utf-8", errors="replace").strip()
            seen.append(login)
            writer.write(b"Hello AI3I-16\r\nAI3I-15> ")
            await writer.drain()
            client_cmd = (await reader.readline()).decode("utf-8", errors="replace").strip()
            seen.append(client_cmd)
            writer.write(b"PC92^AI3I-15^0^D^^WB3FFV-2^H96^\r\n")
            await writer.drain()
            await asyncio.sleep(0.05)
            writer.close()
            await writer.wait_closed()

        try:
            server = await asyncio.start_server(_handler, "127.0.0.1", 0)
        except OSError as exc:
            pytest.skip(f"socket bind not available in this environment: {exc}")
        try:
            sock = (server.sockets or [None])[0]
            assert sock is not None
            port = int(sock.getsockname()[1])
            conn = await connect_from_dsn("legacy", f"dxspider://127.0.0.1:{port}?login=AI3I-16&client=AI3I-15")
            try:
                line = await conn.readline()
                assert line == "PC92^AI3I-15^0^D^^WB3FFV-2^H96^"
                assert seen == ["AI3I-16", "client AI3I-15 telnet"]
            finally:
                await conn.close()
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(run())


def test_dxspider_connect_accepts_direct_pc_banner_without_client_command() -> None:
    async def run() -> None:
        seen: list[str] = []

        async def _handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            writer.write(b"login: ")
            await writer.drain()
            login = (await reader.readline()).decode("utf-8", errors="replace").strip()
            seen.append(login)
            writer.write(b"PC18^DXSpider Version: 1.55 Build: 0.203 Git: 448838ed[r] pc9x^5455^\r\n")
            await writer.drain()
            reply_pc18 = (await reader.readline()).decode("utf-8", errors="replace").strip()
            seen.append(reply_pc18)
            pc20 = (await reader.readline()).decode("utf-8", errors="replace").strip()
            seen.append(pc20)
            await asyncio.sleep(0.05)
            writer.close()
            await writer.wait_closed()

        try:
            server = await asyncio.start_server(_handler, "127.0.0.1", 0)
        except OSError as exc:
            pytest.skip(f"socket bind not available in this environment: {exc}")
        try:
            sock = (server.sockets or [None])[0]
            assert sock is not None
            port = int(sock.getsockname()[1])
            conn = await connect_from_dsn("legacy", f"dxspider://127.0.0.1:{port}?login=AI3I-16&client=AI3I-15")
            try:
                line = await conn.readline()
                assert line == "PC18^DXSpider Version: 1.55 Build: 0.203 Git: 448838ed[r] pc9x^5455^"
                assert seen == [
                    "AI3I-16",
                    "PC18^DXSpider Version: 1.57 Build: 46 Git: pyCluster/1.0.5^5455^",
                    "PC20^",
                ]
            finally:
                await conn.close()
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(run())


def test_dxspider_connect_direct_pc18_keeps_followup_init_frames() -> None:
    async def run() -> None:
        seen: list[str] = []

        async def _handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            writer.write(b"login: ")
            await writer.drain()
            login = (await reader.readline()).decode("utf-8", errors="replace").strip()
            seen.append(login)
            writer.write(b"PC18^DXSpider Version: 1.57 Build: 46 Git: mojo/63d4718 pc9x^5455^\r\n")
            await writer.drain()
            reply_pc18 = (await reader.readline()).decode("utf-8", errors="replace").strip()
            seen.append(reply_pc18)
            pc20 = (await reader.readline()).decode("utf-8", errors="replace").strip()
            seen.append(pc20)
            writer.write(b"PC19^1^AI3I-15^0^1057^H96^\r\n")
            await writer.drain()
            await asyncio.sleep(0.05)
            writer.close()
            await writer.wait_closed()

        try:
            server = await asyncio.start_server(_handler, "127.0.0.1", 0)
        except OSError as exc:
            pytest.skip(f"socket bind not available in this environment: {exc}")
        try:
            sock = (server.sockets or [None])[0]
            assert sock is not None
            port = int(sock.getsockname()[1])
            conn = await connect_from_dsn("legacy", f"dxspider://127.0.0.1:{port}?login=AI3I-16&client=AI3I-15")
            try:
                assert await conn.readline() == "PC18^DXSpider Version: 1.57 Build: 46 Git: mojo/63d4718 pc9x^5455^"
                assert await conn.readline() == "PC19^1^AI3I-15^0^1057^H96^"
                assert seen == [
                    "AI3I-16",
                    "PC18^DXSpider Version: 1.57 Build: 46 Git: pyCluster/1.0.5^5455^",
                    "PC20^",
                ]
            finally:
                await conn.close()
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(run())


def test_dxspider_connect_requires_password_when_prompted() -> None:
    async def run() -> None:
        async def _handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            writer.write(b"login: ")
            await writer.drain()
            await reader.readline()
            writer.write(b"password: ")
            await writer.drain()
            await asyncio.sleep(0.05)
            writer.close()
            await writer.wait_closed()

        try:
            server = await asyncio.start_server(_handler, "127.0.0.1", 0)
        except OSError as exc:
            pytest.skip(f"socket bind not available in this environment: {exc}")
        try:
            sock = (server.sockets or [None])[0]
            assert sock is not None
            port = int(sock.getsockname()[1])
            with pytest.raises(RuntimeError, match="requested password"):
                await connect_from_dsn("legacy", f"dxspider://127.0.0.1:{port}?login=AI3I-16&client=AI3I-15")
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(run())
