import asyncio
from datetime import datetime, timezone

from pycluster.config import AppConfig, NodeConfig, TelnetConfig, WebConfig, PublicWebConfig, StoreConfig
from pycluster.telnet_server import TelnetClusterServer
from pycluster.web_admin import WebAdminServer
from pycluster.public_web import PublicWebServer

from pycluster.address_policy import client_address, network
from pycluster.store import SpotStore


def test_proxy_chain_stops_at_untrusted_hop():
    trust = ['127.0.0.1/32', '::1/128']
    assert client_address(('192.0.2.4', 123), '198.51.100.8', trust) == '192.0.2.4'
    assert client_address(('127.0.0.1', 123), '198.51.100.8, 192.0.2.4', trust) == '192.0.2.4'
    assert client_address(('::1', 123), '2001:db8::8', trust) == '2001:db8::8'
    assert client_address(('127.0.0.1', 123), '198.51.100.8, invalid', trust) == '127.0.0.1'
    assert str(network('::ffff:192.0.2.4/120')) == '192.0.2.0/24'


def test_blocks_persist_expire_and_retain_removal_history(tmp_path, monkeypatch):
    async def run():
        monkeypatch.setattr('pycluster.store.time.time', lambda: 1000)
        path = str(tmp_path / 'blocks.db')
        store = SpotStore(path)
        block = await store.add_address_block('192.0.2.7/24', 'test', 'AI3I-99', 1)
        await store.add_address_block('2001:db8::/64', 'test v6', 'AI3I-99')
        await store.close()
        store = SpotStore(path)
        try:
            assert await store.address_blocked('192.0.2.255')
            assert await store.address_blocked('::ffff:192.0.2.5')
            assert not await store.address_blocked('192.0.3.0')
            assert await store.address_blocked('2001:db8::1')
            assert not await store.address_blocked('2001:db8:0:1::1')
            monkeypatch.setattr('pycluster.store.time.time', lambda: 1060)
            assert not await store.address_blocked('192.0.2.1')
            assert await store.remove_address_block(block, 'AI3I-90')
            rows = await store.list_address_blocks(history=True)
            assert rows[-1]['removed_by'] == 'AI3I-90'
            assert len(await store.list_address_blocks()) == 1
        finally:
            await store.close()
    asyncio.run(run())


def test_blocked_clients_rejected_before_login(tmp_path):
    class Writer:
        def __init__(self):
            self.data = bytearray()
            self.closed = False

        def get_extra_info(self, name, default=None):
            return ('192.0.2.8', 1234) if name == 'peername' else default

        def write(self, data):
            self.data.extend(data)

        async def drain(self):
            pass

        def close(self):
            self.closed = True

        async def wait_closed(self):
            pass

    async def run():
        path = str(tmp_path / 'ingress.db')
        cfg = AppConfig(node=NodeConfig(), telnet=TelnetConfig(), web=WebConfig(), public_web=PublicWebConfig(), store=StoreConfig(sqlite_path=path))
        store = SpotStore(path)
        await store.add_address_block('192.0.2.0/24', 'ingress test', 'AI3I-99')
        try:
            telnet = TelnetClusterServer(config=cfg, store=store, started_at=datetime.now(timezone.utc))
            writer = Writer()
            await telnet._handle_client(asyncio.StreamReader(), writer)
            assert writer.closed and not writer.data
            for server in (
                WebAdminServer(config=cfg, store=store, started_at=datetime.now(timezone.utc), session_count_fn=lambda: 0),
                PublicWebServer(config=cfg, store=store, started_at=datetime.now(timezone.utc)),
            ):
                reader = asyncio.StreamReader()
                reader.feed_data(b'POST /api/auth/login HTTP/1.1\r\nHost: test\r\nX-Forwarded-For: 198.51.100.1\r\nContent-Length: 999\r\n\r\n')
                writer = Writer()
                await asyncio.wait_for(server._handle(reader, writer), 1)
                assert b'403' in writer.data
            assert await store.get_user_registry('AI3I-99') is None
        finally:
            await store.close()
    asyncio.run(run())


def test_telnet_management_requires_sysop(tmp_path):
    async def run():
        path = str(tmp_path / 'commands.db')
        cfg = AppConfig(node=NodeConfig(), telnet=TelnetConfig(), web=WebConfig(), public_web=PublicWebConfig(), store=StoreConfig(sqlite_path=path))
        store = SpotStore(path)
        server = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            await store.upsert_user_registry('AI3I-99', 1000, privilege='user')
            await server._execute_command('AI3I-99', 'sysop/ipblock add 192.0.2.0/24 0 test')
            assert not await store.list_address_blocks()
            await store.upsert_user_registry('AI3I-99', 1000, privilege='sysop')
            await server._execute_command('AI3I-99', 'sysop/ipblock add 192.0.2.0/24 0 test')
            assert await store.address_blocked('192.0.2.99')
            row = (await store.list_address_blocks())[0]
            await server._execute_command('AI3I-99', f"sysop/ipblock remove {row['id']}")
            assert not await store.address_blocked('192.0.2.99')
        finally:
            await store.close()
    asyncio.run(run())
