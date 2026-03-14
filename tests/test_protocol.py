from pycluster.protocol import decode_typed, parse_debug_pc_frame, serialize_debug_pc_frame


def test_parse_debug_frame() -> None:
    line = "1772323200^<- I WB3FFV-2 PC61^1928.0^Z66BCC^ 1-Mar-2026^0000Z^ ^DL6NBC^DA0BCC-7^84.163.40.20^H28^~"
    frame = parse_debug_pc_frame(line)

    assert frame is not None
    assert frame.epoch == 1772323200
    assert frame.link == "WB3FFV-2"
    assert frame.pc_type == "PC61"
    assert frame.payload_fields[0] == "1928.0"
    assert frame.payload_fields[1] == "Z66BCC"
    assert serialize_debug_pc_frame(frame) == line

    typed = decode_typed(frame)
    assert typed is not None
    assert typed.dx_call == "Z66BCC"


def test_parse_non_frame() -> None:
    assert parse_debug_pc_frame("1772335401^Start Protocol Engines ...") is None
