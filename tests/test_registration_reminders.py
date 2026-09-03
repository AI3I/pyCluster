from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace

from pycluster.store import SpotStore
from pycluster.strings import StringCatalog


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "send_registration_reminders.py"
    spec = importlib.util.spec_from_file_location("registration_reminder_script", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Mailer:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.sent: list[tuple[str, str, str]] = []

    def enabled(self) -> bool:
        return True

    def send_code(self, recipient: str, subject: str, body: str) -> None:
        if self.fail:
            raise RuntimeError("mail unavailable")
        self.sent.append((recipient, subject, body))


def test_registration_reminders_use_persisted_paced_stages(tmp_path) -> None:
    async def run() -> None:
        module = _load_module()
        store = SpotStore(str(tmp_path / "reminders.db"))
        config = SimpleNamespace(node=SimpleNamespace(support_contact="sysop@example.test"), smtp=SimpleNamespace())
        mailer = _Mailer()
        requested = 1_800_000_000
        try:
            await store.upsert_registration_request(
                "AI3I-90", requested, email="ai3i-90@example.test", email_verified=True, status="pending"
            )

            result = await module.send_due_reminders(
                store, config, StringCatalog(), now_epoch=requested + 86400, mailer=mailer
            )
            assert result["sent"] == 1
            assert "reminder 1 of 5" in mailer.sent[-1][2]

            result = await module.send_due_reminders(
                store, config, StringCatalog(), now_epoch=requested + 2 * 86400, mailer=mailer
            )
            assert result["sent"] == 0

            result = await module.send_due_reminders(
                store, config, StringCatalog(), now_epoch=requested + 8 * 86400, mailer=mailer
            )
            assert result["sent"] == 1
            assert "reminder 3 of 5" in mailer.sent[-1][2]

            result = await module.send_due_reminders(
                store, config, StringCatalog(), now_epoch=requested + 15 * 86400, mailer=mailer
            )
            assert result["sent"] == 1
            assert "reminder 5 of 5" in mailer.sent[-1][2]

            result = await module.send_due_reminders(
                store, config, StringCatalog(), now_epoch=requested + 30 * 86400, mailer=mailer
            )
            assert result["sent"] == 0
            assert len(mailer.sent) == 3
        finally:
            await store.close()

    asyncio.run(run())


def test_registration_reminder_failure_does_not_advance_stage(tmp_path) -> None:
    async def run() -> None:
        module = _load_module()
        store = SpotStore(str(tmp_path / "reminder_failure.db"))
        config = SimpleNamespace(node=SimpleNamespace(support_contact=""), smtp=SimpleNamespace())
        requested = 1_800_000_000
        try:
            await store.upsert_registration_request(
                "AI3I-91", requested, email="ai3i-91@example.test", email_verified=True, status="pending"
            )
            result = await module.send_due_reminders(
                store, config, StringCatalog(), now_epoch=requested + 4 * 86400, mailer=_Mailer(fail=True)
            )
            assert result["failed"] == 1
            row = await store.get_registration_request("AI3I-91")
            assert row is not None and int(row["reminder_stage"]) == 0
        finally:
            await store.close()

    asyncio.run(run())


def test_new_request_after_review_resets_reminder_schedule(tmp_path) -> None:
    async def run() -> None:
        store = SpotStore(str(tmp_path / "reminder_reset.db"))
        first = 1_800_000_000
        try:
            await store.upsert_registration_request("AI3I-92", first, email="ai3i-92@example.test", status="pending")
            assert await store.mark_registration_reminder_sent("AI3I-92", stage=3, epoch=first + 8 * 86400)
            await store.set_registration_request_status("AI3I-92", status="denied", epoch=first + 9 * 86400)
            second = first + 20 * 86400
            await store.upsert_registration_request("AI3I-92", second, email="ai3i-92@example.test", status="pending")
            row = await store.get_registration_request("AI3I-92")
            assert row is not None
            assert int(row["requested_epoch"]) == second
            assert int(row["reminder_stage"]) == 0
            assert int(row["reminder_epoch"]) == 0
        finally:
            await store.close()

    asyncio.run(run())
