from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import ipaddress
import json
import re
from urllib.parse import urlparse
import uuid

from .models import is_valid_call, normalize_call


PY_PROTOCOL_VERSION = "2"
PY_HELLO_TYPE = "PY00"
PY_NODEINFO_TYPE = "PY01"
PY_TOPOLOGY_DIGEST_TYPE = "PY02"
PY_TOPOLOGY_RECORDS_TYPE = "PY03"
PY_HEALTH_TYPE = "PY04"
PY_DATASETS_TYPE = "PY05"
PY_RBN_STATUS_TYPE = "PY06"
PY_NOTICE_TYPE = "PY07"
PY_POLICY_TYPE = "PY08"
PY_CLOCK_TYPE = "PY09"
PY_REQUEST_TYPE = "PY10"
PY_SESSION_FRAME_TYPE = "PY11"
PY_PROBE_TYPE = "PY12"
PY_WITHDRAW_TYPE = "PY13"
PY_ERROR_TYPE = "PY99"
PY_CAPABILITIES = ("probe", "py99-error", "session-binding")
PY_FRAME_CAPABILITIES = {
    "PY01": "node-info",
    "PY02": "topology-digest",
    "PY03": "topology-records",
    "PY04": "health",
    "PY05": "datasets",
    "PY06": "rbn-status",
    "PY07": "notice",
    "PY08": "policy",
    "PY09": "clock",
    "PY10": "request",
    "PY11": "session-binding",
    "PY12": "probe",
    "PY13": "topology-withdraw",
    PY_ERROR_TYPE: "py99-error",
}
_CAPABILITY_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,31}$")
_ERROR_CODE_RE = re.compile(r"^[a-z][a-z0-9-]{0,31}$")


@dataclass(frozen=True, slots=True)
class PySessionFrameMessage:
    session_id: str
    sequence: int
    inner_type: str
    inner_fields: tuple[str, ...]
    protocol_version: str = PY_PROTOCOL_VERSION

    @classmethod
    def from_fields(cls, fields: list[str]) -> "PySessionFrameMessage":
        if len(fields) != 3 or fields[0].strip() != PY_PROTOCOL_VERSION or fields[1].strip().upper() != "FRAME":
            raise ValueError("invalid PY11 FRAME field layout")
        payload = _decode_payload(fields[2])
        if set(payload) != {"session_id", "sequence", "inner_type", "inner_fields"}:
            raise ValueError("PY11 FRAME contains unknown or missing fields")
        try:
            session_id = str(uuid.UUID(str(payload["session_id"])))
            sequence = int(payload["sequence"])
        except (TypeError, ValueError) as exc:
            raise ValueError("PY11 FRAME session or sequence is invalid") from exc
        inner_type = str(payload["inner_type"]).strip().upper()
        inner_fields_raw = payload["inner_fields"]
        if (
            sequence <= 0
            or not re.fullmatch(r"PY\d{2}", inner_type)
            or inner_type in {PY_HELLO_TYPE, PY_SESSION_FRAME_TYPE}
            or not isinstance(inner_fields_raw, list)
            or not 1 <= len(inner_fields_raw) <= 16
            or any(not isinstance(item, str) or len(item) > 16384 for item in inner_fields_raw)
        ):
            raise ValueError("PY11 FRAME inner frame is invalid")
        return cls(session_id, sequence, inner_type, tuple(inner_fields_raw), fields[0].strip())

    def to_fields(self) -> list[str]:
        payload = {
            "session_id": self.session_id,
            "sequence": self.sequence,
            "inner_type": self.inner_type,
            "inner_fields": list(self.inner_fields),
        }
        fields = [self.protocol_version, "FRAME", _encode_payload(payload)]
        valid = self.from_fields(fields)
        return [valid.protocol_version, "FRAME", _encode_payload(payload)]


def _clean_text(value: object, max_length: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) > max_length or any(ord(char) < 32 for char in text):
        raise ValueError("PY metadata text is invalid")
    return text


def _encode_payload(payload: dict[str, object]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_payload(encoded: str) -> dict[str, object]:
    token = encoded.strip()
    if not token or len(token) > 16384 or not re.fullmatch(r"[A-Za-z0-9_-]+", token):
        raise ValueError("PY structured payload is invalid")
    try:
        raw = base64.b64decode(token + "=" * (-len(token) % 4), altchars=b"-_", validate=True)
        payload = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("PY structured payload is invalid") from exc
    if not isinstance(payload, dict):
        raise ValueError("PY structured payload must be an object")
    return payload


@dataclass(frozen=True, slots=True)
class PyProbeMessage:
    node_call: str
    kind: str
    nonce: str
    sent_millis: int
    reply_millis: int = 0
    protocol_version: str = PY_PROTOCOL_VERSION

    @classmethod
    def from_fields(cls, fields: list[str]) -> "PyProbeMessage":
        if len(fields) != 3 or fields[1].strip().upper() != "PROBE":
            raise ValueError("invalid PY12 PROBE field layout")
        if fields[0].strip() != PY_PROTOCOL_VERSION:
            raise ValueError(f"unsupported PY protocol version: {fields[0].strip()}")
        payload = _decode_payload(fields[2])
        if set(payload) != {"node_call", "kind", "nonce", "sent_millis", "reply_millis"}:
            raise ValueError("PY12 PROBE contains unknown or missing fields")
        node_call = normalize_call(str(payload["node_call"]))
        kind = str(payload["kind"]).strip().lower()
        try:
            nonce = str(uuid.UUID(str(payload["nonce"])))
            sent = int(payload["sent_millis"])
            reply = int(payload["reply_millis"])
        except (TypeError, ValueError) as exc:
            raise ValueError("PY12 PROBE values are invalid") from exc
        if not is_valid_call(node_call) or kind not in {"request", "reply"} or sent <= 0 or reply < 0:
            raise ValueError("PY12 PROBE values are invalid")
        if (kind == "request" and reply != 0) or (kind == "reply" and reply <= 0):
            raise ValueError("PY12 PROBE timing is invalid")
        return cls(node_call, kind, nonce, sent, reply, fields[0].strip())

    def to_fields(self) -> list[str]:
        payload = {"node_call": self.node_call, "kind": self.kind, "nonce": self.nonce,
                   "sent_millis": self.sent_millis, "reply_millis": self.reply_millis}
        fields = [self.protocol_version, "PROBE", _encode_payload(payload)]
        valid = self.from_fields(fields)
        return [valid.protocol_version, "PROBE", _encode_payload({
            "node_call": valid.node_call, "kind": valid.kind, "nonce": valid.nonce,
            "sent_millis": valid.sent_millis, "reply_millis": valid.reply_millis,
        })]


@dataclass(frozen=True, slots=True)
class PyWithdrawMessage:
    reporter_node: str
    node_call: str
    node_id: str
    reason: str
    epoch: int
    protocol_version: str = PY_PROTOCOL_VERSION

    @classmethod
    def from_fields(cls, fields: list[str]) -> "PyWithdrawMessage":
        if len(fields) != 3 or fields[1].strip().upper() != "WITHDRAW":
            raise ValueError("invalid PY13 WITHDRAW field layout")
        if fields[0].strip() != PY_PROTOCOL_VERSION:
            raise ValueError(f"unsupported PY protocol version: {fields[0].strip()}")
        payload = _decode_payload(fields[2])
        if set(payload) != {"reporter_node", "node_call", "node_id", "reason", "epoch"}:
            raise ValueError("PY13 WITHDRAW contains unknown or missing fields")
        reporter = normalize_call(str(payload["reporter_node"]))
        node_call = normalize_call(str(payload["node_call"]))
        reason = str(payload["reason"]).strip().lower()
        try:
            node_id = str(uuid.UUID(str(payload["node_id"])))
            epoch = int(payload["epoch"])
        except (TypeError, ValueError) as exc:
            raise ValueError("PY13 WITHDRAW values are invalid") from exc
        if not is_valid_call(reporter) or not is_valid_call(node_call) or reason not in {"disconnect", "expired", "replaced"} or epoch <= 0:
            raise ValueError("PY13 WITHDRAW values are invalid")
        return cls(reporter, node_call, node_id, reason, epoch, fields[0].strip())

    def to_fields(self) -> list[str]:
        payload = {"reporter_node": self.reporter_node, "node_call": self.node_call,
                   "node_id": self.node_id, "reason": self.reason, "epoch": self.epoch}
        fields = [self.protocol_version, "WITHDRAW", _encode_payload(payload)]
        valid = self.from_fields(fields)
        return [valid.protocol_version, "WITHDRAW", _encode_payload({
            "reporter_node": valid.reporter_node, "node_call": valid.node_call,
            "node_id": valid.node_id, "reason": valid.reason, "epoch": valid.epoch,
        })]


@dataclass(frozen=True, slots=True)
class PyHelloMessage:
    node_call: str
    software_version: str
    capabilities: tuple[str, ...]
    epoch: int
    session_id: str = "00000000-0000-4000-8000-000000000000"
    max_frame_bytes: int = 16384
    max_records_per_frame: int = 50
    max_hops: int = 8
    protocol_version: str = PY_PROTOCOL_VERSION

    @classmethod
    def from_fields(cls, fields: list[str]) -> "PyHelloMessage":
        if len(fields) != 3 or fields[1].strip().upper() != "HELLO":
            raise ValueError("invalid PY00 HELLO field layout")
        protocol_version = fields[0].strip()
        if protocol_version != PY_PROTOCOL_VERSION:
            raise ValueError(f"unsupported PY protocol version: {protocol_version}")
        payload = _decode_payload(fields[2])
        if set(payload) != {"node_call", "software_version", "capabilities", "epoch", "session_id", "limits"}:
            raise ValueError("PY00 HELLO contains unknown or missing fields")
        node_call = normalize_call(str(payload["node_call"]))
        if not is_valid_call(node_call):
            raise ValueError("PY00 HELLO requires a valid node callsign")
        software_version = str(payload["software_version"]).strip()
        if not software_version or len(software_version) > 32:
            raise ValueError("PY00 HELLO requires a software version")
        if not isinstance(payload["capabilities"], list):
            raise ValueError("PY00 HELLO contains an invalid capability list")
        capabilities = tuple(sorted({str(item).strip().lower() for item in payload["capabilities"]}))
        if len(capabilities) != len(payload["capabilities"]) or len(capabilities) > 32 or any(not _CAPABILITY_RE.fullmatch(item) for item in capabilities):
            raise ValueError("PY00 HELLO contains an invalid capability")
        try:
            epoch = int(payload["epoch"])
            session_id = str(uuid.UUID(str(payload["session_id"])))
            limits = payload["limits"]
            if not isinstance(limits, dict) or set(limits) != {"max_frame_bytes", "max_records_per_frame", "max_hops"}:
                raise ValueError
            frame_bytes = int(limits["max_frame_bytes"])
            records = int(limits["max_records_per_frame"])
            hops = int(limits["max_hops"])
        except (TypeError, ValueError) as exc:
            raise ValueError("PY00 HELLO session, epoch, or limits are invalid") from exc
        if epoch <= 0 or not 256 <= frame_bytes <= 65536 or not 1 <= records <= 100 or not 1 <= hops <= 32:
            raise ValueError("PY00 HELLO session, epoch, or limits are invalid")
        return cls(
            node_call=node_call,
            software_version=software_version,
            capabilities=capabilities,
            epoch=epoch,
            session_id=session_id,
            max_frame_bytes=frame_bytes,
            max_records_per_frame=records,
            max_hops=hops,
            protocol_version=protocol_version,
        )

    def to_fields(self) -> list[str]:
        if self.protocol_version != PY_PROTOCOL_VERSION:
            raise ValueError(f"unsupported PY protocol version: {self.protocol_version}")
        node_call = normalize_call(self.node_call)
        if not is_valid_call(node_call):
            raise ValueError("PY00 HELLO requires a valid node callsign")
        software_version = self.software_version.strip()
        capabilities = tuple(sorted(set(self.capabilities)))
        if not software_version or len(software_version) > 32:
            raise ValueError("PY00 HELLO requires a software version")
        if len(capabilities) > 32 or any(not _CAPABILITY_RE.fullmatch(item) for item in capabilities):
            raise ValueError("PY00 HELLO contains an invalid capability")
        payload = {
            "node_call": node_call, "software_version": software_version,
            "capabilities": list(capabilities), "epoch": int(self.epoch),
            "session_id": self.session_id,
            "limits": {"max_frame_bytes": int(self.max_frame_bytes),
                       "max_records_per_frame": int(self.max_records_per_frame),
                       "max_hops": int(self.max_hops)},
        }
        fields = [self.protocol_version, "HELLO", _encode_payload(payload)]
        valid = self.from_fields(fields)
        payload["session_id"] = valid.session_id
        return [valid.protocol_version, "HELLO", _encode_payload(payload)]


@dataclass(frozen=True, slots=True)
class PyErrorMessage:
    code: str
    offending_type: str
    detail: str
    epoch: int
    protocol_version: str = PY_PROTOCOL_VERSION

    @classmethod
    def from_fields(cls, fields: list[str]) -> "PyErrorMessage":
        if len(fields) != 6 or fields[1].strip().upper() != "ERROR":
            raise ValueError("invalid PY99 ERROR field layout")
        protocol_version = fields[0].strip()
        code = fields[2].strip().lower()
        offending_type = fields[3].strip().upper()
        detail = fields[4].strip()
        if protocol_version != PY_PROTOCOL_VERSION:
            raise ValueError(f"unsupported PY protocol version: {protocol_version}")
        if not _ERROR_CODE_RE.fullmatch(code):
            raise ValueError("PY99 ERROR contains an invalid code")
        if not re.fullmatch(r"PY\d{2}[A-Z]?", offending_type):
            raise ValueError("PY99 ERROR contains an invalid frame type")
        if len(detail) > 96 or "^" in detail:
            raise ValueError("PY99 ERROR detail is invalid")
        try:
            epoch = int(fields[5])
        except ValueError as exc:
            raise ValueError("PY99 ERROR epoch must be an integer") from exc
        if epoch <= 0:
            raise ValueError("PY99 ERROR epoch must be positive")
        return cls(code, offending_type, detail, epoch, protocol_version)

    def to_fields(self) -> list[str]:
        return PyErrorMessage.from_fields(
            [
                self.protocol_version,
                "ERROR",
                self.code,
                self.offending_type,
                self.detail,
                str(int(self.epoch)),
            ]
        )._validated_fields()

    def _validated_fields(self) -> list[str]:
        return [
            self.protocol_version,
            "ERROR",
            self.code.strip().lower(),
            self.offending_type.strip().upper(),
            self.detail.strip(),
            str(int(self.epoch)),
        ]


@dataclass(frozen=True, slots=True)
class PyNodeInfoMessage:
    node_call: str
    node_id: str
    sequence: int
    software_version: str
    public_web_url: str
    locator: str
    qth: str
    sysop_contact: str
    services: tuple[str, ...]
    capabilities: tuple[str, ...]
    updated_epoch: int
    expires_epoch: int
    direct_peers: tuple[str, ...] = ()
    protocol_version: str = PY_PROTOCOL_VERSION

    @classmethod
    def from_fields(cls, fields: list[str]) -> "PyNodeInfoMessage":
        if len(fields) != 3 or fields[1].strip().upper() != "NODEINFO":
            raise ValueError("invalid PY01 NODEINFO field layout")
        protocol_version = fields[0].strip()
        if protocol_version != PY_PROTOCOL_VERSION:
            raise ValueError(f"unsupported PY protocol version: {protocol_version}")
        payload = _decode_payload(fields[2])
        expected_keys = {
            "capabilities",
            "direct_peers",
            "expires_epoch",
            "locator",
            "node_call",
            "node_id",
            "public_web_url",
            "qth",
            "sequence",
            "services",
            "software_version",
            "sysop_contact",
            "updated_epoch",
        }
        if set(payload) != expected_keys:
            raise ValueError("PY01 NODEINFO contains unknown or missing fields")
        node_call = normalize_call(str(payload["node_call"]))
        if not is_valid_call(node_call):
            raise ValueError("PY01 NODEINFO requires a valid node callsign")
        try:
            node_id = str(uuid.UUID(str(payload["node_id"])))
            sequence = int(payload["sequence"])
            updated_epoch = int(payload["updated_epoch"])
            expires_epoch = int(payload["expires_epoch"])
        except (TypeError, ValueError) as exc:
            raise ValueError("PY01 NODEINFO contains invalid identity or sequence data") from exc
        if sequence <= 0 or updated_epoch <= 0 or expires_epoch <= updated_epoch:
            raise ValueError("PY01 NODEINFO contains invalid sequence or expiry data")
        if expires_epoch - updated_epoch > 30 * 86400:
            raise ValueError("PY01 NODEINFO expiry exceeds the protocol limit")
        software_version = _clean_text(payload["software_version"], 32)
        if not software_version:
            raise ValueError("PY01 NODEINFO requires a software version")
        public_web_url = _clean_text(payload["public_web_url"], 256)
        if public_web_url:
            parsed_url = urlparse(public_web_url)
            if (
                parsed_url.scheme not in {"http", "https"}
                or not parsed_url.netloc
                or parsed_url.username is not None
                or parsed_url.password is not None
            ):
                raise ValueError("PY01 NODEINFO public web URL is invalid")
            hostname = (parsed_url.hostname or "").lower()
            if hostname == "localhost" or hostname.endswith(".local"):
                raise ValueError("PY01 NODEINFO public web URL is not public")
            try:
                address = ipaddress.ip_address(hostname)
            except ValueError:
                address = None
            if address is not None and not address.is_global:
                raise ValueError("PY01 NODEINFO public web URL is not public")
        locator = _clean_text(payload["locator"], 12).upper()
        if locator and not re.fullmatch(r"[A-R]{2}[0-9]{2}(?:[A-X]{2})?(?:[0-9]{2})?", locator):
            raise ValueError("PY01 NODEINFO locator is invalid")
        qth = _clean_text(payload["qth"], 96)
        sysop_contact = _clean_text(payload["sysop_contact"], 128)
        services = cls._tokens(payload["services"], "service")
        capabilities = cls._tokens(payload["capabilities"], "capability")
        raw_peers = payload["direct_peers"]
        if not isinstance(raw_peers, list) or len(raw_peers) > 100:
            raise ValueError("PY01 NODEINFO direct peer list is invalid")
        direct_peers = tuple(sorted({normalize_call(str(item)) for item in raw_peers}))
        if len(direct_peers) != len(raw_peers) or any(not is_valid_call(item) or item == node_call for item in direct_peers):
            raise ValueError("PY01 NODEINFO direct peer list is invalid")
        return cls(
            node_call,
            node_id,
            sequence,
            software_version,
            public_web_url,
            locator,
            qth,
            sysop_contact,
            services,
            capabilities,
            updated_epoch,
            expires_epoch,
            direct_peers,
            protocol_version,
        )

    @staticmethod
    def _tokens(value: object, label: str) -> tuple[str, ...]:
        if not isinstance(value, list) or len(value) > 32:
            raise ValueError(f"PY01 NODEINFO {label} list is invalid")
        tokens = tuple(sorted({str(item).strip().lower() for item in value if str(item).strip()}))
        if len(tokens) != len(value) or any(not _CAPABILITY_RE.fullmatch(item) for item in tokens):
            raise ValueError(f"PY01 NODEINFO {label} list is invalid")
        return tokens

    def to_fields(self) -> list[str]:
        payload = {
            "node_call": self.node_call,
            "node_id": self.node_id,
            "sequence": self.sequence,
            "software_version": self.software_version,
            "public_web_url": self.public_web_url,
            "locator": self.locator,
            "qth": self.qth,
            "sysop_contact": self.sysop_contact,
            "services": list(self.services),
            "capabilities": list(self.capabilities),
            "updated_epoch": self.updated_epoch,
            "expires_epoch": self.expires_epoch,
            "direct_peers": list(self.direct_peers),
        }
        encoded = _encode_payload(payload)
        validated = PyNodeInfoMessage.from_fields([self.protocol_version, "NODEINFO", encoded])
        canonical_payload = {
            "node_call": validated.node_call,
            "node_id": validated.node_id,
            "sequence": validated.sequence,
            "software_version": validated.software_version,
            "public_web_url": validated.public_web_url,
            "locator": validated.locator,
            "qth": validated.qth,
            "sysop_contact": validated.sysop_contact,
            "services": list(validated.services),
            "capabilities": list(validated.capabilities),
            "updated_epoch": validated.updated_epoch,
            "expires_epoch": validated.expires_epoch,
            "direct_peers": list(validated.direct_peers),
        }
        return [self.protocol_version, "NODEINFO", _encode_payload(canonical_payload)]

    def content_digest(self) -> str:
        content = {
            "node_call": normalize_call(self.node_call),
            "node_id": str(uuid.UUID(self.node_id)),
            "software_version": self.software_version.strip(),
            "public_web_url": self.public_web_url.strip(),
            "locator": self.locator.strip().upper(),
            "qth": " ".join(self.qth.split()),
            "sysop_contact": " ".join(self.sysop_contact.split()),
            "services": sorted(set(self.services)),
            "capabilities": sorted(set(self.capabilities)),
            "direct_peers": sorted(set(self.direct_peers)),
        }
        return hashlib.sha256(
            json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class PyTopologyDigestEntry:
    node_call: str
    node_id: str
    sequence: int
    digest: str
    expires_epoch: int

    @classmethod
    def from_payload(cls, payload: object) -> "PyTopologyDigestEntry":
        if not isinstance(payload, dict) or set(payload) != {
            "node_call", "node_id", "sequence", "digest", "expires_epoch"
        }:
            raise ValueError("PY02 digest entry is invalid")
        node_call = normalize_call(str(payload["node_call"]))
        try:
            node_id = str(uuid.UUID(str(payload["node_id"])))
            sequence = int(payload["sequence"])
            expires_epoch = int(payload["expires_epoch"])
        except (TypeError, ValueError) as exc:
            raise ValueError("PY02 digest identity is invalid") from exc
        digest = str(payload["digest"]).strip().lower()
        if (
            not is_valid_call(node_call)
            or sequence <= 0
            or expires_epoch <= 0
            or not re.fullmatch(r"[0-9a-f]{64}", digest)
        ):
            raise ValueError("PY02 digest entry is invalid")
        return cls(node_call, node_id, sequence, digest, expires_epoch)

    def to_payload(self) -> dict[str, object]:
        valid = self.from_payload(
            {
                "node_call": self.node_call,
                "node_id": self.node_id,
                "sequence": self.sequence,
                "digest": self.digest,
                "expires_epoch": self.expires_epoch,
            }
        )
        return {
            "node_call": valid.node_call,
            "node_id": valid.node_id,
            "sequence": valid.sequence,
            "digest": valid.digest,
            "expires_epoch": valid.expires_epoch,
        }


@dataclass(frozen=True, slots=True)
class PyTopologyDigestMessage:
    entries: tuple[PyTopologyDigestEntry, ...]
    cursor: str
    more: bool
    generated_epoch: int
    snapshot_id: str
    page_number: int
    protocol_version: str = PY_PROTOCOL_VERSION

    @classmethod
    def from_fields(cls, fields: list[str]) -> "PyTopologyDigestMessage":
        if len(fields) != 3 or fields[1].strip().upper() != "TOPOLOGY_DIGEST":
            raise ValueError("invalid PY02 TOPOLOGY_DIGEST field layout")
        if fields[0].strip() != PY_PROTOCOL_VERSION:
            raise ValueError(f"unsupported PY protocol version: {fields[0].strip()}")
        payload = _decode_payload(fields[2])
        if set(payload) != {"cursor", "entries", "generated_epoch", "more", "snapshot_id", "page_number"}:
            raise ValueError("PY02 TOPOLOGY_DIGEST contains unknown or missing fields")
        raw_entries = payload["entries"]
        if not isinstance(raw_entries, list) or len(raw_entries) > 100:
            raise ValueError("PY02 TOPOLOGY_DIGEST entry list is invalid")
        entries = tuple(PyTopologyDigestEntry.from_payload(item) for item in raw_entries)
        if len({item.node_call for item in entries}) != len(entries):
            raise ValueError("PY02 TOPOLOGY_DIGEST contains duplicate nodes")
        cursor = _clean_text(payload["cursor"], 32)
        if cursor and not re.fullmatch(r"[A-Z0-9-]+", cursor):
            raise ValueError("PY02 TOPOLOGY_DIGEST cursor is invalid")
        if not isinstance(payload["more"], bool):
            raise ValueError("PY02 TOPOLOGY_DIGEST continuation flag is invalid")
        if (entries and cursor != entries[-1].node_call) or (not entries and cursor) or (payload["more"] and not entries):
            raise ValueError("PY02 TOPOLOGY_DIGEST cursor does not match the page")
        try:
            generated_epoch = int(payload["generated_epoch"])
            snapshot_id = str(uuid.UUID(str(payload["snapshot_id"])))
            page_number = int(payload["page_number"])
        except (TypeError, ValueError) as exc:
            raise ValueError("PY02 TOPOLOGY_DIGEST snapshot or epoch is invalid") from exc
        if generated_epoch <= 0 or page_number <= 0 or page_number > 10000:
            raise ValueError("PY02 TOPOLOGY_DIGEST snapshot or epoch is invalid")
        return cls(
            entries, cursor, payload["more"], generated_epoch, snapshot_id, page_number, fields[0].strip()
        )

    def to_fields(self) -> list[str]:
        payload = {
            "cursor": self.cursor,
            "entries": [entry.to_payload() for entry in self.entries],
            "generated_epoch": int(self.generated_epoch),
            "more": bool(self.more),
            "snapshot_id": self.snapshot_id,
            "page_number": int(self.page_number),
        }
        encoded = _encode_payload(payload)
        validated = self.from_fields([self.protocol_version, "TOPOLOGY_DIGEST", encoded])
        return [validated.protocol_version, "TOPOLOGY_DIGEST", _encode_payload(payload)]


@dataclass(frozen=True, slots=True)
class PyTopologyRequestMessage:
    node_calls: tuple[str, ...]
    epoch: int
    protocol_version: str = PY_PROTOCOL_VERSION

    @classmethod
    def from_fields(cls, fields: list[str]) -> "PyTopologyRequestMessage":
        if len(fields) != 3 or fields[1].strip().upper() != "REQUEST":
            raise ValueError("invalid PY10 REQUEST field layout")
        if fields[0].strip() != PY_PROTOCOL_VERSION:
            raise ValueError(f"unsupported PY protocol version: {fields[0].strip()}")
        payload = _decode_payload(fields[2])
        if set(payload) != {"epoch", "node_calls"}:
            raise ValueError("PY10 REQUEST contains unknown or missing fields")
        raw_calls = payload["node_calls"]
        if not isinstance(raw_calls, list) or not raw_calls or len(raw_calls) > 100:
            raise ValueError("PY10 REQUEST node list is invalid")
        node_calls = tuple(normalize_call(str(item)) for item in raw_calls)
        if len(set(node_calls)) != len(node_calls) or any(not is_valid_call(item) for item in node_calls):
            raise ValueError("PY10 REQUEST node list is invalid")
        try:
            epoch = int(payload["epoch"])
        except (TypeError, ValueError) as exc:
            raise ValueError("PY10 REQUEST epoch is invalid") from exc
        if epoch <= 0:
            raise ValueError("PY10 REQUEST epoch is invalid")
        return cls(node_calls, epoch, fields[0].strip())

    def to_fields(self) -> list[str]:
        payload = {"epoch": int(self.epoch), "node_calls": list(self.node_calls)}
        encoded = _encode_payload(payload)
        validated = self.from_fields([self.protocol_version, "REQUEST", encoded])
        canonical = {"epoch": validated.epoch, "node_calls": list(validated.node_calls)}
        return [validated.protocol_version, "REQUEST", _encode_payload(canonical)]


@dataclass(frozen=True, slots=True)
class PyTopologyRecord:
    node_info: PyNodeInfoMessage
    origin_node: str
    hop_count: int
    digest: str

    @classmethod
    def from_payload(cls, payload: object) -> "PyTopologyRecord":
        if not isinstance(payload, dict):
            raise ValueError("PY03 topology record is invalid")
        info_keys = {
            "capabilities", "direct_peers", "expires_epoch", "locator", "node_call", "node_id",
            "public_web_url", "qth", "sequence", "services", "software_version",
            "sysop_contact", "updated_epoch",
        }
        if set(payload) != info_keys | {"origin_node", "hop_count", "digest"}:
            raise ValueError("PY03 topology record contains unknown or missing fields")
        info_payload = {key: payload[key] for key in info_keys}
        info = PyNodeInfoMessage.from_fields(
            [PY_PROTOCOL_VERSION, "NODEINFO", _encode_payload(info_payload)]
        )
        origin_node = normalize_call(str(payload["origin_node"]))
        try:
            hop_count = int(payload["hop_count"])
        except (TypeError, ValueError) as exc:
            raise ValueError("PY03 topology hop count is invalid") from exc
        digest = str(payload["digest"]).strip().lower()
        if origin_node != info.node_call or hop_count < 0 or hop_count > 32:
            raise ValueError("PY03 topology origin is invalid")
        if digest != info.content_digest():
            raise ValueError("PY03 topology digest does not match its content")
        return cls(info, origin_node, hop_count, digest)

    def to_payload(self) -> dict[str, object]:
        info = PyNodeInfoMessage.from_fields(self.node_info.to_fields())
        payload = {
            "node_call": info.node_call,
            "node_id": info.node_id,
            "sequence": info.sequence,
            "software_version": info.software_version,
            "public_web_url": info.public_web_url,
            "locator": info.locator,
            "qth": info.qth,
            "sysop_contact": info.sysop_contact,
            "services": list(info.services),
            "capabilities": list(info.capabilities),
            "direct_peers": list(info.direct_peers),
            "updated_epoch": info.updated_epoch,
            "expires_epoch": info.expires_epoch,
            "origin_node": self.origin_node,
            "hop_count": int(self.hop_count),
            "digest": self.digest,
        }
        return self.from_payload(payload)._raw_payload()

    def _raw_payload(self) -> dict[str, object]:
        info = self.node_info
        return {
            "node_call": info.node_call, "node_id": info.node_id, "sequence": info.sequence,
            "software_version": info.software_version, "public_web_url": info.public_web_url,
            "locator": info.locator, "qth": info.qth, "sysop_contact": info.sysop_contact,
            "services": list(info.services), "capabilities": list(info.capabilities),
            "direct_peers": list(info.direct_peers),
            "updated_epoch": info.updated_epoch, "expires_epoch": info.expires_epoch,
            "origin_node": self.origin_node, "hop_count": self.hop_count, "digest": self.digest,
        }


@dataclass(frozen=True, slots=True)
class PyTopologyRecordsMessage:
    records: tuple[PyTopologyRecord, ...]
    epoch: int
    protocol_version: str = PY_PROTOCOL_VERSION

    @classmethod
    def from_fields(cls, fields: list[str]) -> "PyTopologyRecordsMessage":
        if len(fields) != 3 or fields[1].strip().upper() != "TOPOLOGY_RECORDS":
            raise ValueError("invalid PY03 TOPOLOGY_RECORDS field layout")
        if fields[0].strip() != PY_PROTOCOL_VERSION:
            raise ValueError(f"unsupported PY protocol version: {fields[0].strip()}")
        payload = _decode_payload(fields[2])
        if set(payload) != {"epoch", "records"}:
            raise ValueError("PY03 TOPOLOGY_RECORDS contains unknown or missing fields")
        raw_records = payload["records"]
        if not isinstance(raw_records, list) or not raw_records or len(raw_records) > 100:
            raise ValueError("PY03 TOPOLOGY_RECORDS record list is invalid")
        records = tuple(PyTopologyRecord.from_payload(item) for item in raw_records)
        if len({item.node_info.node_call for item in records}) != len(records):
            raise ValueError("PY03 TOPOLOGY_RECORDS contains duplicate nodes")
        try:
            epoch = int(payload["epoch"])
        except (TypeError, ValueError) as exc:
            raise ValueError("PY03 TOPOLOGY_RECORDS epoch is invalid") from exc
        if epoch <= 0:
            raise ValueError("PY03 TOPOLOGY_RECORDS epoch is invalid")
        return cls(records, epoch, fields[0].strip())

    def to_fields(self) -> list[str]:
        payload = {"epoch": int(self.epoch), "records": [record.to_payload() for record in self.records]}
        encoded = _encode_payload(payload)
        validated = self.from_fields([self.protocol_version, "TOPOLOGY_RECORDS", encoded])
        canonical = {"epoch": validated.epoch, "records": [record._raw_payload() for record in validated.records]}
        return [validated.protocol_version, "TOPOLOGY_RECORDS", _encode_payload(canonical)]


def _status_envelope(
    payload: dict[str, object], expected_keys: set[str], label: str, *, max_ttl_seconds: int = 86400
) -> tuple[str, int, int]:
    if set(payload) != expected_keys | {"node_call", "generated_epoch", "expires_epoch"}:
        raise ValueError(f"{label} contains unknown or missing fields")
    node_call = normalize_call(str(payload["node_call"]))
    try:
        generated_epoch = int(payload["generated_epoch"])
        expires_epoch = int(payload["expires_epoch"])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} timing is invalid") from exc
    if (
        not is_valid_call(node_call)
        or generated_epoch <= 0
        or expires_epoch <= generated_epoch
        or expires_epoch - generated_epoch > max_ttl_seconds
    ):
        raise ValueError(f"{label} identity or timing is invalid")
    return node_call, generated_epoch, expires_epoch


def _status_fields(protocol_version: str, message_type: str, payload: dict[str, object]) -> list[str]:
    if protocol_version != PY_PROTOCOL_VERSION:
        raise ValueError(f"unsupported PY protocol version: {protocol_version}")
    return [protocol_version, message_type, _encode_payload(payload)]


@dataclass(frozen=True, slots=True)
class PyHealthMessage:
    node_call: str
    state: str
    services: tuple[tuple[str, str], ...]
    link_state: str
    last_rx_epoch: int
    last_tx_epoch: int
    receive_quiet: bool
    transmit_active: bool
    flapping: bool
    reconnecting: bool
    last_error_category: str
    generated_epoch: int
    expires_epoch: int
    protocol_version: str = PY_PROTOCOL_VERSION

    @classmethod
    def from_fields(cls, fields: list[str]) -> "PyHealthMessage":
        if len(fields) != 3 or fields[1].strip().upper() != "HEALTH":
            raise ValueError("invalid PY04 HEALTH field layout")
        if fields[0].strip() != PY_PROTOCOL_VERSION:
            raise ValueError(f"unsupported PY protocol version: {fields[0].strip()}")
        payload = _decode_payload(fields[2])
        node_call, generated, expires = _status_envelope(
            payload,
            {"state", "services", "link_state", "last_rx_epoch", "last_tx_epoch", "receive_quiet",
             "transmit_active", "flapping", "reconnecting", "last_error_category"},
            "PY04 HEALTH",
        )
        state = str(payload["state"]).strip().lower()
        link_state = str(payload["link_state"]).strip().lower()
        if state not in {"healthy", "degraded", "maintenance"}:
            raise ValueError("PY04 HEALTH node state is invalid")
        if link_state not in {"connected", "quiet", "stale", "flapping", "reconnecting"}:
            raise ValueError("PY04 HEALTH link state is invalid")
        raw_services = payload["services"]
        if not isinstance(raw_services, dict) or len(raw_services) > 16:
            raise ValueError("PY04 HEALTH service state is invalid")
        services = tuple(sorted((_clean_text(k, 32).lower(), _clean_text(v, 24).lower()) for k, v in raw_services.items()))
        if any(not _CAPABILITY_RE.fullmatch(k) or v not in {"up", "down", "disabled", "degraded"} for k, v in services):
            raise ValueError("PY04 HEALTH service state is invalid")
        try:
            last_rx = int(payload["last_rx_epoch"])
            last_tx = int(payload["last_tx_epoch"])
        except (TypeError, ValueError) as exc:
            raise ValueError("PY04 HEALTH activity time is invalid") from exc
        flags = [payload[key] for key in ("receive_quiet", "transmit_active", "flapping", "reconnecting")]
        if (
            last_rx < 0 or last_tx < 0
            or last_rx > generated + 300 or last_tx > generated + 300
            or any(not isinstance(flag, bool) for flag in flags)
        ):
            raise ValueError("PY04 HEALTH activity state is invalid")
        error = _clean_text(payload["last_error_category"], 48).lower()
        return cls(node_call, state, services, link_state, last_rx, last_tx, *flags, error, generated, expires, fields[0].strip())

    def to_fields(self) -> list[str]:
        payload = {
            "node_call": self.node_call, "state": self.state, "services": dict(self.services),
            "link_state": self.link_state, "last_rx_epoch": self.last_rx_epoch,
            "last_tx_epoch": self.last_tx_epoch, "receive_quiet": self.receive_quiet,
            "transmit_active": self.transmit_active, "flapping": self.flapping,
            "reconnecting": self.reconnecting, "last_error_category": self.last_error_category,
            "generated_epoch": self.generated_epoch, "expires_epoch": self.expires_epoch,
        }
        fields = _status_fields(self.protocol_version, "HEALTH", payload)
        valid = self.from_fields(fields)
        canonical = {
            "node_call": valid.node_call, "state": valid.state, "services": dict(valid.services),
            "link_state": valid.link_state, "last_rx_epoch": valid.last_rx_epoch,
            "last_tx_epoch": valid.last_tx_epoch, "receive_quiet": valid.receive_quiet,
            "transmit_active": valid.transmit_active, "flapping": valid.flapping,
            "reconnecting": valid.reconnecting, "last_error_category": valid.last_error_category,
            "generated_epoch": valid.generated_epoch, "expires_epoch": valid.expires_epoch,
        }
        return _status_fields(valid.protocol_version, "HEALTH", canonical)


@dataclass(frozen=True, slots=True)
class PyDatasetsMessage:
    node_call: str
    datasets: tuple[tuple[str, str, str, int, bool, str], ...]
    generated_epoch: int
    expires_epoch: int
    protocol_version: str = PY_PROTOCOL_VERSION

    @classmethod
    def from_fields(cls, fields: list[str]) -> "PyDatasetsMessage":
        if len(fields) != 3 or fields[1].strip().upper() != "DATASETS":
            raise ValueError("invalid PY05 DATASETS field layout")
        if fields[0].strip() != PY_PROTOCOL_VERSION:
            raise ValueError(f"unsupported PY protocol version: {fields[0].strip()}")
        payload = _decode_payload(fields[2])
        node_call, generated, expires = _status_envelope(payload, {"datasets"}, "PY05 DATASETS")
        raw = payload["datasets"]
        if not isinstance(raw, list) or len(raw) > 16:
            raise ValueError("PY05 DATASETS list is invalid")
        datasets = []
        for item in raw:
            if not isinstance(item, dict) or set(item) != {"name", "version", "version_date", "modified_epoch", "stale", "status"}:
                raise ValueError("PY05 DATASETS entry is invalid")
            try:
                modified = int(item["modified_epoch"])
            except (TypeError, ValueError) as exc:
                raise ValueError("PY05 DATASETS modified time is invalid") from exc
            name = _clean_text(item["name"], 32).lower()
            status = _clean_text(item["status"], 32).lower()
            if modified < 0 or modified > generated + 300 or not isinstance(item["stale"], bool) or not name or not status:
                raise ValueError("PY05 DATASETS entry is invalid")
            datasets.append((name, _clean_text(item["version"], 48),
                             _clean_text(item["version_date"], 24), modified, item["stale"],
                             status))
        if len({item[0] for item in datasets}) != len(datasets):
            raise ValueError("PY05 DATASETS contains duplicate entries")
        return cls(node_call, tuple(sorted(datasets)), generated, expires, fields[0].strip())

    def to_fields(self) -> list[str]:
        payload = {"node_call": self.node_call, "datasets": [
            {"name": n, "version": v, "version_date": vd, "modified_epoch": m, "stale": s, "status": st}
            for n, v, vd, m, s, st in self.datasets
        ], "generated_epoch": self.generated_epoch, "expires_epoch": self.expires_epoch}
        fields = _status_fields(self.protocol_version, "DATASETS", payload)
        valid = self.from_fields(fields)
        canonical = {"node_call": valid.node_call, "datasets": [
            {"name": n, "version": v, "version_date": vd, "modified_epoch": m, "stale": s, "status": st}
            for n, v, vd, m, s, st in valid.datasets
        ], "generated_epoch": valid.generated_epoch, "expires_epoch": valid.expires_epoch}
        return _status_fields(valid.protocol_version, "DATASETS", canonical)


@dataclass(frozen=True, slots=True)
class PyRbnStatusMessage:
    node_call: str
    enabled: bool
    modes: tuple[str, ...]
    feed_count: int
    connected_count: int
    state: str
    last_spot_epoch: int
    recent_spots_per_minute: int
    queue_state: str
    generated_epoch: int
    expires_epoch: int
    protocol_version: str = PY_PROTOCOL_VERSION

    @classmethod
    def from_fields(cls, fields: list[str]) -> "PyRbnStatusMessage":
        if len(fields) != 3 or fields[1].strip().upper() != "RBN_STATUS":
            raise ValueError("invalid PY06 RBN_STATUS field layout")
        if fields[0].strip() != PY_PROTOCOL_VERSION:
            raise ValueError(f"unsupported PY protocol version: {fields[0].strip()}")
        payload = _decode_payload(fields[2])
        node_call, generated, expires = _status_envelope(
            payload, {"enabled", "modes", "feed_count", "connected_count", "state", "last_spot_epoch",
                      "recent_spots_per_minute", "queue_state"}, "PY06 RBN_STATUS"
        )
        if not isinstance(payload["enabled"], bool) or not isinstance(payload["modes"], list):
            raise ValueError("PY06 RBN_STATUS configuration is invalid")
        modes = tuple(sorted({_clean_text(item, 16).upper() for item in payload["modes"]}))
        if (
            len(modes) != len(payload["modes"])
            or len(modes) > 16
            or any(not re.fullmatch(r"[A-Z0-9-]{1,16}", mode) for mode in modes)
        ):
            raise ValueError("PY06 RBN_STATUS mode list is invalid")
        try:
            feed_count = int(payload["feed_count"])
            connected = int(payload["connected_count"])
            last_spot = int(payload["last_spot_epoch"])
            rate = int(payload["recent_spots_per_minute"])
        except (TypeError, ValueError) as exc:
            raise ValueError("PY06 RBN_STATUS count is invalid") from exc
        state = _clean_text(payload["state"], 24).lower()
        queue_state = _clean_text(payload["queue_state"], 24).lower()
        if (
            feed_count < 0 or connected < 0 or connected > feed_count
            or last_spot < 0 or last_spot > generated + 300 or rate < 0
        ):
            raise ValueError("PY06 RBN_STATUS count is invalid")
        if state not in {"disabled", "stopped", "starting", "connected", "degraded", "error"} or queue_state not in {"normal", "backpressure", "unknown"}:
            raise ValueError("PY06 RBN_STATUS state is invalid")
        return cls(node_call, payload["enabled"], modes, feed_count, connected, state, last_spot, rate,
                   queue_state, generated, expires, fields[0].strip())

    def to_fields(self) -> list[str]:
        payload = {"node_call": self.node_call, "enabled": self.enabled, "modes": list(self.modes),
                   "feed_count": self.feed_count, "connected_count": self.connected_count, "state": self.state,
                   "last_spot_epoch": self.last_spot_epoch, "recent_spots_per_minute": self.recent_spots_per_minute,
                   "queue_state": self.queue_state, "generated_epoch": self.generated_epoch,
                   "expires_epoch": self.expires_epoch}
        fields = _status_fields(self.protocol_version, "RBN_STATUS", payload)
        valid = self.from_fields(fields)
        canonical = {"node_call": valid.node_call, "enabled": valid.enabled, "modes": list(valid.modes),
                     "feed_count": valid.feed_count, "connected_count": valid.connected_count, "state": valid.state,
                     "last_spot_epoch": valid.last_spot_epoch,
                     "recent_spots_per_minute": valid.recent_spots_per_minute,
                     "queue_state": valid.queue_state, "generated_epoch": valid.generated_epoch,
                     "expires_epoch": valid.expires_epoch}
        return _status_fields(valid.protocol_version, "RBN_STATUS", canonical)


@dataclass(frozen=True, slots=True)
class PyNoticeMessage:
    node_call: str
    notice_id: str
    sequence: int
    active: bool
    severity: str
    message: str
    created_epoch: int
    generated_epoch: int
    expires_epoch: int
    protocol_version: str = PY_PROTOCOL_VERSION

    @classmethod
    def from_fields(cls, fields: list[str]) -> "PyNoticeMessage":
        if len(fields) != 3 or fields[1].strip().upper() != "NOTICE":
            raise ValueError("invalid PY07 NOTICE field layout")
        if fields[0].strip() != PY_PROTOCOL_VERSION:
            raise ValueError(f"unsupported PY protocol version: {fields[0].strip()}")
        payload = _decode_payload(fields[2])
        node_call, generated, expires = _status_envelope(
            payload,
            {"notice_id", "sequence", "active", "severity", "message", "created_epoch"},
            "PY07 NOTICE",
            max_ttl_seconds=30 * 86400,
        )
        try:
            notice_id = str(uuid.UUID(str(payload["notice_id"])))
            sequence = int(payload["sequence"])
            created_epoch = int(payload["created_epoch"])
        except (TypeError, ValueError) as exc:
            raise ValueError("PY07 NOTICE identity is invalid") from exc
        if not isinstance(payload["active"], bool):
            raise ValueError("PY07 NOTICE active state is invalid")
        severity = _clean_text(payload["severity"], 16).lower()
        message = _clean_text(payload["message"], 240)
        if severity not in {"normal", "maintenance", "upgrading", "degraded", "testing"}:
            raise ValueError("PY07 NOTICE severity is invalid")
        if sequence <= 0 or created_epoch <= 0 or created_epoch > generated + 300:
            raise ValueError("PY07 NOTICE sequence or creation time is invalid")
        if payload["active"] and not message:
            raise ValueError("PY07 NOTICE active records require a message")
        if not payload["active"] and message:
            raise ValueError("PY07 NOTICE inactive records cannot carry a message")
        return cls(
            node_call, notice_id, sequence, payload["active"], severity, message,
            created_epoch, generated, expires, fields[0].strip(),
        )

    def to_fields(self) -> list[str]:
        payload = {
            "node_call": self.node_call, "notice_id": self.notice_id, "sequence": self.sequence,
            "active": self.active, "severity": self.severity, "message": self.message,
            "created_epoch": self.created_epoch, "generated_epoch": self.generated_epoch,
            "expires_epoch": self.expires_epoch,
        }
        fields = _status_fields(self.protocol_version, "NOTICE", payload)
        valid = self.from_fields(fields)
        canonical = {
            "node_call": valid.node_call, "notice_id": valid.notice_id, "sequence": valid.sequence,
            "active": valid.active, "severity": valid.severity, "message": valid.message,
            "created_epoch": valid.created_epoch, "generated_epoch": valid.generated_epoch,
            "expires_epoch": valid.expires_epoch,
        }
        return _status_fields(valid.protocol_version, "NOTICE", canonical)


@dataclass(frozen=True, slots=True)
class PyPolicyMessage:
    node_call: str
    registration_required: bool
    email_verification_web: bool
    email_verification_telnet: bool
    mfa_available: bool
    mfa_required_users: bool
    mfa_required_sysops: bool
    public_web_enabled: bool
    anonymous_web_enabled: bool
    generated_epoch: int
    expires_epoch: int
    protocol_version: str = PY_PROTOCOL_VERSION

    @classmethod
    def from_fields(cls, fields: list[str]) -> "PyPolicyMessage":
        if len(fields) != 3 or fields[1].strip().upper() != "POLICY":
            raise ValueError("invalid PY08 POLICY field layout")
        if fields[0].strip() != PY_PROTOCOL_VERSION:
            raise ValueError(f"unsupported PY protocol version: {fields[0].strip()}")
        keys = {"registration_required", "email_verification_web", "email_verification_telnet", "mfa_available",
                "mfa_required_users", "mfa_required_sysops", "public_web_enabled", "anonymous_web_enabled"}
        payload = _decode_payload(fields[2])
        node_call, generated, expires = _status_envelope(payload, keys, "PY08 POLICY")
        values = [payload[key] for key in sorted(keys)]
        if any(not isinstance(value, bool) for value in values):
            raise ValueError("PY08 POLICY values must be boolean")
        return cls(node_call, payload["registration_required"], payload["email_verification_web"],
                   payload["email_verification_telnet"], payload["mfa_available"], payload["mfa_required_users"],
                   payload["mfa_required_sysops"], payload["public_web_enabled"], payload["anonymous_web_enabled"],
                   generated, expires, fields[0].strip())

    def to_fields(self) -> list[str]:
        payload = {"node_call": self.node_call, "registration_required": self.registration_required,
                   "email_verification_web": self.email_verification_web,
                   "email_verification_telnet": self.email_verification_telnet, "mfa_available": self.mfa_available,
                   "mfa_required_users": self.mfa_required_users, "mfa_required_sysops": self.mfa_required_sysops,
                   "public_web_enabled": self.public_web_enabled, "anonymous_web_enabled": self.anonymous_web_enabled,
                   "generated_epoch": self.generated_epoch, "expires_epoch": self.expires_epoch}
        fields = _status_fields(self.protocol_version, "POLICY", payload)
        valid = self.from_fields(fields)
        canonical = {"node_call": valid.node_call, "registration_required": valid.registration_required,
                     "email_verification_web": valid.email_verification_web,
                     "email_verification_telnet": valid.email_verification_telnet,
                     "mfa_available": valid.mfa_available, "mfa_required_users": valid.mfa_required_users,
                     "mfa_required_sysops": valid.mfa_required_sysops,
                     "public_web_enabled": valid.public_web_enabled,
                     "anonymous_web_enabled": valid.anonymous_web_enabled,
                     "generated_epoch": valid.generated_epoch, "expires_epoch": valid.expires_epoch}
        return _status_fields(valid.protocol_version, "POLICY", canonical)


@dataclass(frozen=True, slots=True)
class PyClockMessage:
    node_call: str
    utc_epoch: int
    uptime_seconds: int
    boot_epoch: int
    generated_epoch: int
    expires_epoch: int
    protocol_version: str = PY_PROTOCOL_VERSION

    @classmethod
    def from_fields(cls, fields: list[str]) -> "PyClockMessage":
        if len(fields) != 3 or fields[1].strip().upper() != "CLOCK":
            raise ValueError("invalid PY09 CLOCK field layout")
        if fields[0].strip() != PY_PROTOCOL_VERSION:
            raise ValueError(f"unsupported PY protocol version: {fields[0].strip()}")
        payload = _decode_payload(fields[2])
        node_call, generated, expires = _status_envelope(payload, {"utc_epoch", "uptime_seconds", "boot_epoch"}, "PY09 CLOCK")
        try:
            utc_epoch = int(payload["utc_epoch"])
            uptime = int(payload["uptime_seconds"])
            boot = int(payload["boot_epoch"])
        except (TypeError, ValueError) as exc:
            raise ValueError("PY09 CLOCK values are invalid") from exc
        if (
            utc_epoch <= 0 or uptime < 0 or boot <= 0 or boot > utc_epoch
            or abs(utc_epoch - generated) > 5
            or abs((utc_epoch - boot) - uptime) > 300
        ):
            raise ValueError("PY09 CLOCK values are invalid")
        return cls(node_call, utc_epoch, uptime, boot, generated, expires, fields[0].strip())

    def to_fields(self) -> list[str]:
        payload = {"node_call": self.node_call, "utc_epoch": self.utc_epoch,
                   "uptime_seconds": self.uptime_seconds, "boot_epoch": self.boot_epoch,
                   "generated_epoch": self.generated_epoch, "expires_epoch": self.expires_epoch}
        fields = _status_fields(self.protocol_version, "CLOCK", payload)
        valid = self.from_fields(fields)
        canonical = {"node_call": valid.node_call, "utc_epoch": valid.utc_epoch,
                     "uptime_seconds": valid.uptime_seconds, "boot_epoch": valid.boot_epoch,
                     "generated_epoch": valid.generated_epoch, "expires_epoch": valid.expires_epoch}
        return _status_fields(valid.protocol_version, "CLOCK", canonical)
