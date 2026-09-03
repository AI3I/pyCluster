#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys
import time


def _bootstrap_import_path() -> None:
    src = Path(__file__).resolve().parent.parent / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))


_bootstrap_import_path()

from pycluster.config import load_config  # noqa: E402
from pycluster.mfa import SMTPMailer  # noqa: E402
from pycluster.registration import has_valid_email  # noqa: E402
from pycluster.store import SpotStore  # noqa: E402
from pycluster.strings import StringCatalog  # noqa: E402


REMINDER_DAYS = (1, 4, 7, 10, 14)


def _due_stage(requested_epoch: int, current_stage: int, now_epoch: int) -> tuple[int, int] | None:
    age_days = max(0, (int(now_epoch) - int(requested_epoch)) // 86400)
    due = [index for index, day in enumerate(REMINDER_DAYS, start=1) if age_days >= day]
    if not due or max(due) <= int(current_stage):
        return None
    return max(due), age_days


async def send_due_reminders(
    store: SpotStore,
    config,
    catalog: StringCatalog,
    *,
    now_epoch: int,
    mailer: SMTPMailer | None = None,
) -> dict[str, object]:
    sender = mailer or SMTPMailer(config.smtp)
    result: dict[str, object] = {"pending": 0, "due": 0, "sent": 0, "failed": 0, "skipped_email": 0}
    rows = await store.list_registration_requests(status="pending", limit=1000)
    result["pending"] = len(rows)
    if not sender.enabled():
        result["mail_configured"] = False
        return result
    result["mail_configured"] = True
    support = config.node.support_contact.strip() or "the node system operator"
    for row in rows:
        due = _due_stage(int(row["requested_epoch"] or 0), int(row["reminder_stage"] or 0), now_epoch)
        if due is None:
            continue
        stage, age_days = due
        result["due"] = int(result["due"]) + 1
        call = str(row["call"] or "").strip().upper()
        email = str(row["email"] or "").strip()
        if not has_valid_email(email):
            result["skipped_email"] = int(result["skipped_email"]) + 1
            continue
        subject = catalog.render(
            "registration.reminder_subject",
            "pyCluster registration request still pending for {call}",
            call=call,
        )
        body = catalog.render(
            "registration.reminder_body",
            (
                "Your pyCluster registration request for {call} has been pending for {age_days} day(s).\n\n"
                "A system operator still needs to review it. Contact {support_contact} if you need assistance.\n\n"
                "This is reminder {stage} of {total}; automatic reminders stop after 14 days.\n"
            ),
            call=call,
            age_days=age_days,
            support_contact=support,
            stage=stage,
            total=len(REMINDER_DAYS),
        )
        try:
            await asyncio.to_thread(sender.send_code, email, subject, body)
        except Exception as exc:
            result["failed"] = int(result["failed"]) + 1
            print(f"registration reminder failed call={call}: {type(exc).__name__}: {exc}", file=sys.stderr)
            continue
        if await store.mark_registration_reminder_sent(call, stage=stage, epoch=now_epoch):
            result["sent"] = int(result["sent"]) + 1
    return result


async def _main_async(args: argparse.Namespace) -> int:
    config_path = Path(args.config).expanduser().resolve()
    project_root = config_path.parent.parent if config_path.parent.name == "config" else config_path.parent
    config = load_config(config_path)
    db_path = Path(config.store.sqlite_path)
    if not db_path.is_absolute():
        db_path = (project_root / db_path).resolve()
    store = SpotStore(str(db_path))
    try:
        result = await send_due_reminders(
            store,
            config,
            StringCatalog(config_path.parent / "strings.toml"),
            now_epoch=int(time.time()),
        )
        print(json.dumps(result, sort_keys=True))
        return 1 if int(result["failed"]) else 0
    finally:
        await store.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Send paced reminders for pending pyCluster registration requests.")
    parser.add_argument("--config", required=True)
    return asyncio.run(_main_async(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
