from pycluster.protocol import WirePcFrame, decode_typed, encode_typed, parse_debug_pc_frame, sanitize_pc92_private_ips, serialize_debug_pc_frame


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


def test_pc92_sanitizer_replaces_private_ipv4_and_ipv6() -> None:
    frame = WirePcFrame(
        "PC92",
        [
            "N9JR-2",
            "42291",
            "A",
            "",
            "7N9JR-4:192.168.222.19",
            "5TEST-1:fd00::1234",
            "7PUB-1:8.8.8.8",
            "H96",
            "",
        ],
    )
    cleaned = sanitize_pc92_private_ips(frame, "44.1.2.3")
    assert cleaned.payload_fields[4] == "7N9JR-4:44.1.2.3"
    assert cleaned.payload_fields[5] == "5TEST-1:44.1.2.3"
    assert cleaned.payload_fields[6] == "7PUB-1:8.8.8.8"


def test_pc92_sanitizer_supports_ipv6_public_replacement() -> None:
    frame = WirePcFrame("PC92", ["N0NODE-1", "1", "A", "", "5LOCAL:[fe80::1]", "H96", ""])
    cleaned = sanitize_pc92_private_ips(frame, "2606:4700:4700::1111")
    assert cleaned.payload_fields[4] == "5LOCAL:[2606:4700:4700::1111]"


def test_pc92_sanitizer_prefers_same_family_replacements() -> None:
    frame = WirePcFrame("PC92", ["N0NODE-1", "1", "A", "", "7V4:10.0.0.4", "5V6:[fd00::4]", "H96", ""])
    cleaned = sanitize_pc92_private_ips(frame, "44.1.2.3", "2606:4700:4700::1111")
    assert cleaned.payload_fields[4] == "7V4:44.1.2.3"
    assert cleaned.payload_fields[5] == "5V6:[2606:4700:4700::1111]"


def test_pc92_sanitizer_replaces_localhost_path_alias() -> None:
    frame = WirePcFrame("PC92", ["N9JR-2", "61551", "A", "", "7N9JR-3:localhost", "H99", ""])
    cleaned = sanitize_pc92_private_ips(frame, "44.1.2.3")
    assert cleaned.payload_fields[4] == "7N9JR-3:44.1.2.3"


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
