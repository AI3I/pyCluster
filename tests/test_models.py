from pycluster.models import is_valid_call, parse_spot_record


def test_callsign_validation() -> None:
    assert is_valid_call("AI3I")
    assert is_valid_call("AI3I-15")
    assert is_valid_call("N0CALL")
    assert is_valid_call("W3NH/B")
    assert is_valid_call("JJ2VAS/P")
    assert is_valid_call("P4/WE9V")
    assert not is_valid_call("bad call")
    assert not is_valid_call("W3NH//B")
    assert not is_valid_call("/W3NH")


def test_parse_spot_record() -> None:
    line = "7109.9^K3AJ^1772335320^RTTY^WW5L^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42"
    spot = parse_spot_record(line)

    assert spot.freq_khz == 7109.9
    assert spot.dx_call == "K3AJ"
    assert spot.epoch == 1772335320
    assert spot.info == "RTTY"
    assert spot.spotter == "WW5L"
    assert spot.source_node == "N2WQ-1"
