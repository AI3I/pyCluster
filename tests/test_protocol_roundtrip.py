from pycluster.protocol import decode_typed, encode_typed, parse_debug_pc_frame, serialize_debug_pc_frame


def _roundtrip(line: str) -> None:
    frame = parse_debug_pc_frame(line)
    assert frame is not None

    typed = decode_typed(frame)
    assert typed is not None

    rebuilt_fields = encode_typed(frame.pc_type, typed)
    frame.payload_fields = rebuilt_fields
    assert serialize_debug_pc_frame(frame) == line


def test_pc92_roundtrip() -> None:
    _roundtrip("1772323200^<- I WB3FFV-2 PC92^UF3K-1^0^D^^5R1BLH-1^H96^")


def test_pc93_roundtrip() -> None:
    _roundtrip(
        "1772323200^<- I WB3FFV-2 PC93^YO3FCA-8^0^*^YO3FCA-8^*^DXspider Node YO3FCA-8 *** Telnet amprnet.ddns.net : 7301 *** CW/RTTY/FTx RBN ***^^127.0.0.1^H94^"
    )


def test_pc11_roundtrip() -> None:
    _roundtrip("1772323226^<- I WB3FFV-2 PC11^7225.0^K4MDI^01-Mar-2026^0000Z^LSB^KD2KW^NC7J^H97^~")


def test_pc23_roundtrip() -> None:
    _roundtrip("1772323240^<- I WB3FFV-2 PC23^14-Mar-2026^18^120^24^4^Moderate w/G2 -> Minor w/G1^W0MU^AI3I-16^H96^")


def test_pc24_roundtrip() -> None:
    _roundtrip("1772323258^<- I WB3FFV-2 PC24^4X0IARC^1^H29^")


def test_pc50_roundtrip() -> None:
    _roundtrip("1772323324^<- I WB3FFV-2 PC50^W3LPL^63^H97^")


def test_pc51_roundtrip() -> None:
    _roundtrip("1772323359^<- I WB3FFV-2 PC51^AI3I-15^WB3FFV-2^1^")


def test_pc73_roundtrip() -> None:
    _roundtrip("1772323380^<- I WB3FFV-2 PC73^14-Mar-2026^20^120^18^3^2^105^qui^maj^no^DK0WCY^AI3I-16^H96^")
