from __future__ import annotations

import re
from dataclasses import dataclass


PC_FRAME_RE = re.compile(
    r"^(?P<epoch>\d+)\^(?P<arrow><-?)\s+(?P<io>[IO])\s+(?P<link>[^\s]+)\s+(?P<pc>PC\d+[A-Z]?)\^(?P<payload>.*)$"
)


@dataclass(slots=True)
class PcFrame:
    epoch: int
    link: str
    pc_type: str
    payload_fields: list[str]
    arrow: str = "<-"
    io: str = "I"


@dataclass(slots=True)
class WirePcFrame:
    pc_type: str
    payload_fields: list[str]


@dataclass(slots=True)
class Pc18Message:
    software: str
    proto_version: str = ""
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc18Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(
            software=get(0),
            proto_version=get(1),
            trailer=get(2),
            raw_fields=list(fields),
        )

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [self.software, self.proto_version, self.trailer]


@dataclass(slots=True)
class Pc10Message:
    from_call: str
    user1: str
    text: str
    star: str
    user2: str
    origin_node: str
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc10Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(
            from_call=get(0),
            user1=get(1),
            text=get(2),
            star=get(3),
            user2=get(4),
            origin_node=get(5),
            trailer=get(6),
            raw_fields=list(fields),
        )

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [
            self.from_call,
            self.user1,
            self.text,
            self.star,
            self.user2,
            self.origin_node,
            self.trailer,
        ]


@dataclass(slots=True)
class Pc28Message:
    to_node: str
    from_node: str
    to_call: str
    from_call: str
    date_token: str
    time_token: str
    private_flag: str
    subject: str
    placeholder1: str
    line_count: str
    rr_flag: str
    placeholder2: str
    origin: str
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc28Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(
            to_node=get(0),
            from_node=get(1),
            to_call=get(2),
            from_call=get(3),
            date_token=get(4),
            time_token=get(5),
            private_flag=get(6),
            subject=get(7),
            placeholder1=get(8),
            line_count=get(9),
            rr_flag=get(10),
            placeholder2=get(11),
            origin=get(12),
            trailer=get(13),
            raw_fields=list(fields),
        )

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [
            self.to_node,
            self.from_node,
            self.to_call,
            self.from_call,
            self.date_token,
            self.time_token,
            self.private_flag,
            self.subject,
            self.placeholder1,
            self.line_count,
            self.rr_flag,
            self.placeholder2,
            self.origin,
            self.trailer,
        ]


@dataclass(slots=True)
class Pc29Message:
    to_node: str
    from_node: str
    stream: str
    text: str
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc29Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(
            to_node=get(0),
            from_node=get(1),
            stream=get(2),
            text=get(3),
            trailer=get(4),
            raw_fields=list(fields),
        )

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [self.to_node, self.from_node, self.stream, self.text, self.trailer]


@dataclass(slots=True)
class Pc30Message:
    to_node: str
    from_node: str
    stream: str
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc30Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(to_node=get(0), from_node=get(1), stream=get(2), trailer=get(3), raw_fields=list(fields))

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [self.to_node, self.from_node, self.stream, self.trailer]


@dataclass(slots=True)
class Pc31Message:
    to_node: str
    from_node: str
    stream: str
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc31Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(to_node=get(0), from_node=get(1), stream=get(2), trailer=get(3), raw_fields=list(fields))

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [self.to_node, self.from_node, self.stream, self.trailer]


@dataclass(slots=True)
class Pc32Message:
    to_node: str
    from_node: str
    stream: str
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc32Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(to_node=get(0), from_node=get(1), stream=get(2), trailer=get(3), raw_fields=list(fields))

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [self.to_node, self.from_node, self.stream, self.trailer]


@dataclass(slots=True)
class Pc33Message:
    to_node: str
    from_node: str
    stream: str
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc33Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(to_node=get(0), from_node=get(1), stream=get(2), trailer=get(3), raw_fields=list(fields))

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [self.to_node, self.from_node, self.stream, self.trailer]


@dataclass(slots=True)
class Pc61Message:
    freq_khz: str
    dx_call: str
    date_token: str
    time_token: str
    info: str
    spotter: str
    source_node: str
    ip: str = ""
    hops_token: str = ""
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc61Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(
            freq_khz=get(0),
            dx_call=get(1),
            date_token=get(2),
            time_token=get(3),
            info=get(4),
            spotter=get(5),
            source_node=get(6),
            ip=get(7),
            hops_token=get(8),
            trailer=get(9),
            raw_fields=list(fields),
        )

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [
            self.freq_khz,
            self.dx_call,
            self.date_token,
            self.time_token,
            self.info,
            self.spotter,
            self.source_node,
            self.ip,
            self.hops_token,
            self.trailer,
        ]


@dataclass(slots=True)
class Pc92Message:
    node_call: str
    metric: str
    event_type: str
    extra: str
    route_info: str
    hops_token: str = ""
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc92Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(
            node_call=get(0),
            metric=get(1),
            event_type=get(2),
            extra=get(3),
            route_info=get(4),
            hops_token=get(5),
            trailer=get(6),
            raw_fields=list(fields),
        )

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [
            self.node_call,
            self.metric,
            self.event_type,
            self.extra,
            self.route_info,
            self.hops_token,
            self.trailer,
        ]


@dataclass(slots=True)
class Pc93Message:
    node_call: str
    metric: str
    star1: str
    origin_call: str
    star2: str
    text: str
    extra: str
    ip: str
    hops_token: str = ""
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc93Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(
            node_call=get(0),
            metric=get(1),
            star1=get(2),
            origin_call=get(3),
            star2=get(4),
            text=get(5),
            extra=get(6),
            ip=get(7),
            hops_token=get(8),
            trailer=get(9),
            raw_fields=list(fields),
        )

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [
            self.node_call,
            self.metric,
            self.star1,
            self.origin_call,
            self.star2,
            self.text,
            self.extra,
            self.ip,
            self.hops_token,
            self.trailer,
        ]


@dataclass(slots=True)
class Pc11Message:
    freq_khz: str
    dx_call: str
    date_token: str
    time_token: str
    info: str
    spotter: str
    source_node: str
    hops_token: str = ""
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc11Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(
            freq_khz=get(0),
            dx_call=get(1),
            date_token=get(2),
            time_token=get(3),
            info=get(4),
            spotter=get(5),
            source_node=get(6),
            hops_token=get(7),
            trailer=get(8),
            raw_fields=list(fields),
        )

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [
            self.freq_khz,
            self.dx_call,
            self.date_token,
            self.time_token,
            self.info,
            self.spotter,
            self.source_node,
            self.hops_token,
            self.trailer,
        ]


@dataclass(slots=True)
class Pc12Message:
    from_call: str
    to_node: str
    text: str
    sysop_flag: str
    origin_node: str
    wx_flag: str
    hops_token: str = ""
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc12Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(
            from_call=get(0),
            to_node=get(1),
            text=get(2),
            sysop_flag=get(3),
            origin_node=get(4),
            wx_flag=get(5),
            hops_token=get(6),
            trailer=get(7),
            raw_fields=list(fields),
        )

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [
            self.from_call,
            self.to_node,
            self.text,
            self.sysop_flag,
            self.origin_node,
            self.wx_flag,
            self.hops_token,
            self.trailer,
        ]


@dataclass(slots=True)
class Pc24Message:
    call: str
    flag: str
    hops_token: str = ""
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc24Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(
            call=get(0),
            flag=get(1),
            hops_token=get(2),
            trailer=get(3),
            raw_fields=list(fields),
        )

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [self.call, self.flag, self.hops_token, self.trailer]


@dataclass(slots=True)
class Pc50Message:
    call: str
    node_count: str
    hops_token: str = ""
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc50Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(
            call=get(0),
            node_count=get(1),
            hops_token=get(2),
            trailer=get(3),
            raw_fields=list(fields),
        )

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [self.call, self.node_count, self.hops_token, self.trailer]


@dataclass(slots=True)
class Pc51Message:
    to_call: str
    from_call: str
    value: str
    trailer: str = ""
    raw_fields: list[str] | None = None

    @classmethod
    def from_fields(cls, fields: list[str]) -> "Pc51Message":
        f = fields
        get = lambda i: f[i] if i < len(f) else ""
        return cls(
            to_call=get(0),
            from_call=get(1),
            value=get(2),
            trailer=get(3),
            raw_fields=list(fields),
        )

    def to_fields(self) -> list[str]:
        if self.raw_fields is not None:
            return list(self.raw_fields)
        return [self.to_call, self.from_call, self.value, self.trailer]


def parse_debug_pc_frame(line: str) -> PcFrame | None:
    m = PC_FRAME_RE.match(line.strip())
    if not m:
        return None
    payload = m.group("payload")
    return PcFrame(
        epoch=int(m.group("epoch")),
        arrow=m.group("arrow"),
        io=m.group("io"),
        link=m.group("link"),
        pc_type=m.group("pc"),
        payload_fields=payload.split("^") if payload else [],
    )


def serialize_debug_pc_frame(frame: PcFrame) -> str:
    payload = "^".join(frame.payload_fields)
    return f"{frame.epoch}^{frame.arrow} {frame.io} {frame.link} {frame.pc_type}^{payload}"


def parse_wire_pc_frame(line: str) -> WirePcFrame | None:
    raw = line.strip()
    if not raw:
        return None
    parts = raw.split("^")
    pc = parts[0].strip().upper()
    if not re.match(r"^PC\d+[A-Z]?$", pc):
        return None
    return WirePcFrame(pc_type=pc, payload_fields=parts[1:])


def serialize_wire_pc_frame(frame: WirePcFrame) -> str:
    return "^".join([frame.pc_type, *frame.payload_fields])


def decode_typed(
    frame: PcFrame,
) -> Pc18Message | Pc10Message | Pc28Message | Pc29Message | Pc30Message | Pc31Message | Pc32Message | Pc33Message | Pc61Message | Pc92Message | Pc93Message | Pc11Message | Pc12Message | Pc24Message | Pc50Message | Pc51Message | None:
    if frame.pc_type == "PC18":
        return Pc18Message.from_fields(frame.payload_fields)
    if frame.pc_type == "PC10":
        return Pc10Message.from_fields(frame.payload_fields)
    if frame.pc_type == "PC28":
        return Pc28Message.from_fields(frame.payload_fields)
    if frame.pc_type == "PC29":
        return Pc29Message.from_fields(frame.payload_fields)
    if frame.pc_type == "PC30":
        return Pc30Message.from_fields(frame.payload_fields)
    if frame.pc_type == "PC31":
        return Pc31Message.from_fields(frame.payload_fields)
    if frame.pc_type == "PC32":
        return Pc32Message.from_fields(frame.payload_fields)
    if frame.pc_type == "PC33":
        return Pc33Message.from_fields(frame.payload_fields)
    if frame.pc_type == "PC61":
        return Pc61Message.from_fields(frame.payload_fields)
    if frame.pc_type == "PC92":
        return Pc92Message.from_fields(frame.payload_fields)
    if frame.pc_type == "PC93":
        return Pc93Message.from_fields(frame.payload_fields)
    if frame.pc_type == "PC11":
        return Pc11Message.from_fields(frame.payload_fields)
    if frame.pc_type == "PC12":
        return Pc12Message.from_fields(frame.payload_fields)
    if frame.pc_type == "PC24":
        return Pc24Message.from_fields(frame.payload_fields)
    if frame.pc_type == "PC50":
        return Pc50Message.from_fields(frame.payload_fields)
    if frame.pc_type == "PC51":
        return Pc51Message.from_fields(frame.payload_fields)
    return None


def encode_typed(
    pc_type: str,
    message: Pc18Message | Pc10Message | Pc28Message | Pc29Message | Pc30Message | Pc31Message | Pc32Message | Pc33Message | Pc61Message | Pc92Message | Pc93Message | Pc11Message | Pc12Message | Pc24Message | Pc50Message | Pc51Message,
) -> list[str]:
    if pc_type == "PC18" and isinstance(message, Pc18Message):
        return message.to_fields()
    if pc_type == "PC10" and isinstance(message, Pc10Message):
        return message.to_fields()
    if pc_type == "PC28" and isinstance(message, Pc28Message):
        return message.to_fields()
    if pc_type == "PC29" and isinstance(message, Pc29Message):
        return message.to_fields()
    if pc_type == "PC30" and isinstance(message, Pc30Message):
        return message.to_fields()
    if pc_type == "PC31" and isinstance(message, Pc31Message):
        return message.to_fields()
    if pc_type == "PC32" and isinstance(message, Pc32Message):
        return message.to_fields()
    if pc_type == "PC33" and isinstance(message, Pc33Message):
        return message.to_fields()
    if pc_type == "PC61" and isinstance(message, Pc61Message):
        return message.to_fields()
    if pc_type == "PC92" and isinstance(message, Pc92Message):
        return message.to_fields()
    if pc_type == "PC93" and isinstance(message, Pc93Message):
        return message.to_fields()
    if pc_type == "PC11" and isinstance(message, Pc11Message):
        return message.to_fields()
    if pc_type == "PC12" and isinstance(message, Pc12Message):
        return message.to_fields()
    if pc_type == "PC24" and isinstance(message, Pc24Message):
        return message.to_fields()
    if pc_type == "PC50" and isinstance(message, Pc50Message):
        return message.to_fields()
    if pc_type == "PC51" and isinstance(message, Pc51Message):
        return message.to_fields()
    raise ValueError(f"unsupported type/instance combination: {pc_type}")
