from pycluster.models import (
    is_plausible_spot_call,
    is_plausible_spotter_call,
    is_valid_call,
    is_valid_registration_call,
    parse_spot_record,
)


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


def test_registration_callsign_validation() -> None:
    assert is_valid_registration_call("AI3I")
    assert is_valid_registration_call("AI3I-15")
    assert is_valid_registration_call("N0CALL")
    assert is_valid_registration_call("I1AA")
    assert is_valid_registration_call("SV9IOI")
    assert is_valid_registration_call("VP2MAA")
    assert is_valid_registration_call("7N9JR")
    assert is_valid_registration_call("3D2AG")
    assert is_valid_registration_call("S50CLX")
    assert not is_valid_registration_call("JOHN")
    assert not is_valid_registration_call("JOHN1A")
    assert not is_valid_registration_call("ADMIN1")
    assert not is_valid_registration_call("USER123")
    assert not is_valid_registration_call("bad call")
    assert not is_valid_registration_call("W3NH/B")
    assert not is_valid_registration_call("P4/WE9V")
    assert not is_valid_registration_call("AI3I-ABC")


def test_rbn_skimmer_spotter_validation() -> None:
    assert is_plausible_spot_call("KO4BHX")
    assert not is_plausible_spot_call("KO4BHX-#")
    assert is_plausible_spotter_call("KO4BHX-#")
    assert not is_plausible_spotter_call("KO4BHX-##")


def test_parse_spot_record() -> None:
    line = "7109.9^K3AJ^1772335320^RTTY^WW5L^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42"
    spot = parse_spot_record(line)

    assert spot.freq_khz == 7109.9
    assert spot.dx_call == "K3AJ"
    assert spot.epoch == 1772335320
    assert spot.info == "RTTY"
    assert spot.spotter == "WW5L"
    assert spot.source_node == "N2WQ-1"
