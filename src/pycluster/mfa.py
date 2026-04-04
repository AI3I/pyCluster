from __future__ import annotations

from dataclasses import dataclass
import secrets
import smtplib
import ssl
import time
from email.message import EmailMessage
from typing import Callable

from .config import MFAConfig, SMTPConfig
from .store import SpotStore


@dataclass(slots=True)
class EmailOtpChallenge:
    challenge_id: str
    call: str
    purpose: str
    code: str
    expires_epoch: int
    attempts_left: int


class SMTPMailer:
    def __init__(self, config: SMTPConfig) -> None:
        self.config = config

    def enabled(self) -> bool:
        return bool(self.config.host.strip() and self.config.from_addr.strip())

    def send_code(self, recipient: str, subject: str, body: str) -> None:
        msg = EmailMessage()
        from_name = self.config.from_name.strip()
        from_addr = self.config.from_addr.strip()
        msg["From"] = f"{from_name} <{from_addr}>" if from_name else from_addr
        msg["To"] = recipient.strip()
        msg["Subject"] = subject
        msg.set_content(body)

        timeout = max(1, int(self.config.timeout_seconds or 10))
        if self.config.use_ssl:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self.config.host, int(self.config.port or 465), timeout=timeout, context=context) as smtp:
                if self.config.username.strip():
                    smtp.login(self.config.username, self.config.password)
                smtp.send_message(msg)
            return

        with smtplib.SMTP(self.config.host, int(self.config.port or 587), timeout=timeout) as smtp:
            if self.config.starttls:
                smtp.starttls(context=ssl.create_default_context())
            if self.config.username.strip():
                smtp.login(self.config.username, self.config.password)
            smtp.send_message(msg)


class EmailOtpManager:
    def __init__(self, config: MFAConfig, sender: Callable[[str, str, str], None], store: SpotStore | None = None) -> None:
        self.config = config
        self._sender = sender
        self._store = store
        self._challenges: dict[str, EmailOtpChallenge] = {}
        self._recent_issue: dict[tuple[str, str], int] = {}

    def enabled(self) -> bool:
        return bool(self.config.enabled)

    def required_for(self, *, is_sysop: bool) -> bool:
        if not self.enabled():
            return False
        if is_sysop:
            return bool(self.config.require_for_sysop)
        return bool(self.config.require_for_users)

    async def issue(self, *, call: str, email: str, purpose: str) -> tuple[str, int]:
        now = int(time.time())
        if self._store is not None:
            await self._store.delete_expired_mfa_challenges(now)
        cooldown = max(0, int(self.config.resend_cooldown_seconds or 0))
        key = (call.upper(), purpose)
        last_issue = self._recent_issue.get(key, 0)
        if cooldown > 0 and last_issue > 0 and now - last_issue < cooldown:
            raise RuntimeError("otp recently issued")
        ttl = max(60, int(self.config.otp_ttl_seconds or 600))
        length = max(6, min(8, int(self.config.otp_length or 6)))
        challenge_id = secrets.token_urlsafe(24)
        digits = "".join(secrets.choice("0123456789") for _ in range(length))
        challenge = EmailOtpChallenge(
            challenge_id=challenge_id,
            call=call.upper(),
            purpose=purpose,
            code=digits,
            expires_epoch=now + ttl,
            attempts_left=max(1, int(self.config.max_attempts or 5)),
        )
        self._challenges[challenge_id] = challenge
        self._recent_issue[key] = now
        if self._store is not None:
            await self._store.save_mfa_challenge(
                challenge_id=challenge_id,
                call=challenge.call,
                purpose=challenge.purpose,
                code=challenge.code,
                expires_epoch=challenge.expires_epoch,
                attempts_left=challenge.attempts_left,
                issued_epoch=now,
            )
        issuer = self.config.issuer.strip() or "pyCluster"
        subject = f"{issuer} login code for {call.upper()}"
        body = (
            f"{issuer} login verification code for {call.upper()}: {digits}\n\n"
            f"This code expires in {ttl // 60} minute(s).\n"
            f"If you did not request this login, ignore this message.\n"
        )
        self._sender(email.strip(), subject, body)
        return challenge_id, challenge.expires_epoch

    async def verify(self, *, challenge_id: str, call: str, purpose: str, otp: str) -> tuple[bool, str]:
        challenge = self._challenges.get(challenge_id)
        if challenge is None and self._store is not None:
            row = await self._store.get_mfa_challenge(challenge_id)
            if row is not None:
                challenge = EmailOtpChallenge(
                    challenge_id=str(row["challenge_id"]),
                    call=str(row["call"]),
                    purpose=str(row["purpose"]),
                    code=str(row["code"]),
                    expires_epoch=int(row["expires_epoch"] or 0),
                    attempts_left=int(row["attempts_left"] or 0),
                )
                self._challenges[challenge_id] = challenge
        if challenge is None:
            return False, "invalid challenge"
        if challenge.call != call.upper() or challenge.purpose != purpose:
            self._challenges.pop(challenge_id, None)
            if self._store is not None:
                await self._store.delete_mfa_challenge(challenge_id)
            return False, "invalid challenge"
        if challenge.expires_epoch < int(time.time()):
            self._challenges.pop(challenge_id, None)
            if self._store is not None:
                await self._store.delete_mfa_challenge(challenge_id)
            return False, "challenge expired"
        if challenge.attempts_left <= 0:
            self._challenges.pop(challenge_id, None)
            if self._store is not None:
                await self._store.delete_mfa_challenge(challenge_id)
            return False, "too many attempts"
        if not secrets.compare_digest(challenge.code, str(otp or "").strip()):
            challenge.attempts_left -= 1
            if challenge.attempts_left <= 0:
                self._challenges.pop(challenge_id, None)
                if self._store is not None:
                    await self._store.delete_mfa_challenge(challenge_id)
                return False, "too many attempts"
            if self._store is not None:
                await self._store.update_mfa_challenge_attempts(challenge_id, challenge.attempts_left)
            return False, "invalid code"
        self._challenges.pop(challenge_id, None)
        if self._store is not None:
            await self._store.delete_mfa_challenge(challenge_id)
        return True, ""
