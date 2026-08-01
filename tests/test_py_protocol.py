import pytest

from pycluster.py_protocol import (
    PY_CAPABILITIES,
    PyErrorMessage,
    PyClockMessage,
    PyDatasetsMessage,
    PyHealthMessage,
    PyHelloMessage,
    PyNodeInfoMessage,
    PyNoticeMessage,
    PyPolicyMessage,
    PyRbnStatusMessage,
    PyTopologyDigestEntry,
    PyTopologyDigestMessage,
    PyTopologyRecord,
    PyTopologyRecordsMessage,
    PyTopologyRequestMessage,
)


def test_py_hello_round_trip_is_canonical() -> None:
    hello = PyHelloMessage(
        node_call="ai3i-90",
        software_version="1.0.12",
        capabilities=tuple(reversed(PY_CAPABILITIES)),
        epoch=1785456000,
    )

    fields = hello.to_fields()
    assert fields == [
        "1",
        "HELLO",
        "AI3I-90",
        "1.0.12",
        "py99-error",
        "1785456000",
    ]
    assert PyHelloMessage.from_fields(fields) == PyHelloMessage(
        node_call="AI3I-90",
        software_version="1.0.12",
        capabilities=PY_CAPABILITIES,
        epoch=1785456000,
    )


@pytest.mark.parametrize(
    "fields",
    [
        ["2", "HELLO", "AI3I-90", "1.0.12", "node-discovery", "1785456000"],
        ["1", "QUERY", "AI3I-90", "1.0.12", "node-discovery", "1785456000"],
        ["1", "HELLO", "", "1.0.12", "node-discovery", "1785456000"],
        ["1", "HELLO", "NOT-A-CALL", "1.0.12", "node-discovery", "1785456000"],
        ["1", "HELLO", "AI3I-90", "", "node-discovery", "1785456000"],
        ["1", "HELLO", "AI3I-90", "1.0.12", "bad capability", "1785456000"],
        ["1", "HELLO", "AI3I-90", "1.0.12", "node-discovery", "not-an-epoch"],
    ],
)
def test_py_hello_rejects_invalid_fields(fields: list[str]) -> None:
    with pytest.raises(ValueError):
        PyHelloMessage.from_fields(fields)


def test_py_error_round_trip() -> None:
    error = PyErrorMessage("unsupported-type", "py42", "Capability was not negotiated", 1785456000)
    fields = error.to_fields()
    assert fields == [
        "1",
        "ERROR",
        "unsupported-type",
        "PY42",
        "Capability was not negotiated",
        "1785456000",
    ]
    assert PyErrorMessage.from_fields(fields) == PyErrorMessage(
        "unsupported-type", "PY42", "Capability was not negotiated", 1785456000
    )


def test_py_node_info_round_trip_is_canonical() -> None:
    message = PyNodeInfoMessage(
        node_call="ai3i-90",
        node_id="12345678-1234-5678-9234-567812345678",
        sequence=3,
        software_version="1.0.12",
        public_web_url="https://pycluster.example.net/",
        locator="fn00fs",
        qth="Western Pennsylvania",
        sysop_contact="sysop@example.net",
        services=("telnet", "public-web"),
        capabilities=("py99-error", "node-info"),
        updated_epoch=1785456000,
        expires_epoch=1785542400,
    )

    fields = message.to_fields()

    assert fields[:2] == ["1", "NODEINFO"]
    decoded = PyNodeInfoMessage.from_fields(fields)
    assert decoded.node_call == "AI3I-90"
    assert decoded.node_id == "12345678-1234-5678-9234-567812345678"
    assert decoded.sequence == 3
    assert decoded.locator == "FN00FS"
    assert decoded.services == ("public-web", "telnet")
    assert decoded.capabilities == ("node-info", "py99-error")
    assert decoded.to_fields() == fields


def test_py_node_info_rejects_invalid_public_metadata() -> None:
    message = PyNodeInfoMessage(
        node_call="AI3I-90",
        node_id="12345678-1234-5678-9234-567812345678",
        sequence=1,
        software_version="1.0.12",
        public_web_url="file:///etc/passwd",
        locator="FN00FS",
        qth="Test",
        sysop_contact="",
        services=("telnet",),
        capabilities=("node-info",),
        updated_epoch=1785456000,
        expires_epoch=1785542400,
    )

    with pytest.raises(ValueError):
        message.to_fields()

    private_url = PyNodeInfoMessage(
        node_call="AI3I-90",
        node_id="12345678-1234-5678-9234-567812345678",
        sequence=1,
        software_version="1.0.12",
        public_web_url="http://192.168.1.10/",
        locator="FN00FS",
        qth="Test",
        sysop_contact="",
        services=("telnet",),
        capabilities=("node-info",),
        updated_epoch=1785456000,
        expires_epoch=1785542400,
    )
    with pytest.raises(ValueError):
        private_url.to_fields()


def test_py_topology_messages_round_trip_canonically() -> None:
    info = PyNodeInfoMessage(
        node_call="AI3I-92",
        node_id="22345678-1234-5678-9234-567812345678",
        sequence=4,
        software_version="1.0.12",
        public_web_url="https://node.example.net/",
        locator="FN00FS",
        qth="Test Node",
        sysop_contact="",
        services=("telnet",),
        capabilities=("topology-digest",),
        updated_epoch=1785456000,
        expires_epoch=1785542400,
    )
    entry = PyTopologyDigestEntry(
        info.node_call, info.node_id, info.sequence, info.content_digest(), info.expires_epoch
    )
    digest = PyTopologyDigestMessage(
        (entry,), "AI3I-92", False, 1785456001,
        "42345678-1234-5678-9234-567812345678", 1,
    )
    request = PyTopologyRequestMessage(("AI3I-92",), 1785456002)
    record = PyTopologyRecord(info, "AI3I-92", 0, info.content_digest())
    records = PyTopologyRecordsMessage((record,), 1785456003)

    assert PyTopologyDigestMessage.from_fields(digest.to_fields()) == digest
    assert PyTopologyRequestMessage.from_fields(request.to_fields()) == request
    assert PyTopologyRecordsMessage.from_fields(records.to_fields()) == records


def test_py_topology_rejects_tampered_record_digest() -> None:
    info = PyNodeInfoMessage(
        "AI3I-92",
        "22345678-1234-5678-9234-567812345678",
        1,
        "1.0.12",
        "",
        "FN00FS",
        "Test",
        "",
        ("telnet",),
        ("topology-records",),
        1785456000,
        1785542400,
    )
    record = PyTopologyRecord(info, "AI3I-92", 0, "0" * 64)
    with pytest.raises(ValueError, match="digest"):
        record.to_payload()


def test_py_operational_metadata_messages_round_trip() -> None:
    messages = (
        PyHealthMessage(
            "AI3I-90", "healthy", (("publicweb", "up"), ("telnet", "up")), "connected",
            1785455990, 1785455995, False, True, False, False, "", 1785456000, 1785456600,
        ),
        PyDatasetsMessage(
            "AI3I-90", (("cty.dat", "VER20260731", "2026-07-31", 1785369600, False, "loaded"),),
            1785456000, 1785456600,
        ),
        PyRbnStatusMessage(
            "AI3I-90", True, ("CW", "FT8", "RTTY"), 2, 2, "connected", 1785455999, 42,
            "normal", 1785456000, 1785456600,
        ),
        PyPolicyMessage(
            "AI3I-90", True, True, False, True, False, True, True, True,
            1785456000, 1785456600,
        ),
        PyClockMessage("AI3I-90", 1785456000, 3600, 1785452400, 1785456000, 1785456600),
    )
    parsers = (
        PyHealthMessage.from_fields,
        PyDatasetsMessage.from_fields,
        PyRbnStatusMessage.from_fields,
        PyPolicyMessage.from_fields,
        PyClockMessage.from_fields,
    )
    for message, parser in zip(messages, parsers, strict=True):
        fields = message.to_fields()
        assert parser(fields) == message
        assert parser(fields).to_fields() == fields


def test_py_operational_metadata_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="node state"):
        PyHealthMessage(
            "AI3I-90", "unknown", (), "connected", 0, 0, True, False, False, False, "",
            1785456000, 1785456600,
        ).to_fields()
    with pytest.raises(ValueError, match="count"):
        PyRbnStatusMessage(
            "AI3I-90", True, ("CW",), 1, 2, "connected", 0, 0, "normal",
            1785456000, 1785456600,
        ).to_fields()
    with pytest.raises(ValueError, match="CLOCK"):
        PyClockMessage("AI3I-90", 1785457000, 0, 1785456000, 1785456000, 1785456600).to_fields()
    with pytest.raises(ValueError, match="timing"):
        PyHealthMessage(
            "AI3I-90", "healthy", (), "connected", 0, 0, False, False, False, False, "",
            1785456000, 1785542401,
        ).to_fields()


def test_py_notice_round_trip_and_cancellation() -> None:
    active = PyNoticeMessage(
        "ai3i-90", "52345678-1234-5678-9234-567812345678", 4, True, "maintenance",
        "Antenna work in progress", 1785455900, 1785456000, 1785459600,
    )
    decoded = PyNoticeMessage.from_fields(active.to_fields())
    assert decoded.node_call == "AI3I-90"
    assert decoded.active is True
    assert decoded.message == "Antenna work in progress"

    cleared = PyNoticeMessage(
        "AI3I-90", "62345678-1234-5678-9234-567812345678", 5, False, "normal", "",
        1785456100, 1785456100, 1785459700,
    )
    assert PyNoticeMessage.from_fields(cleared.to_fields()) == cleared


def test_py_notice_rejects_ambiguous_state() -> None:
    with pytest.raises(ValueError, match="require a message"):
        PyNoticeMessage(
            "AI3I-90", "52345678-1234-5678-9234-567812345678", 1, True, "degraded", "",
            1785456000, 1785456000, 1785459600,
        ).to_fields()
    with pytest.raises(ValueError, match="cannot carry"):
        PyNoticeMessage(
            "AI3I-90", "52345678-1234-5678-9234-567812345678", 2, False, "normal", "Old",
            1785456000, 1785456000, 1785459600,
        ).to_fields()
    with pytest.raises(ValueError, match="timing"):
        PyNoticeMessage(
            "AI3I-90", "52345678-1234-5678-9234-567812345678", 3, True, "normal", "Too long",
            1785456000, 1785456000, 1788048001,
        ).to_fields()


def test_py_notice_allows_a_thirty_day_lease() -> None:
    notice = PyNoticeMessage(
        "AI3I-90", "52345678-1234-5678-9234-567812345678", 1, True, "normal", "Planned event",
        1785456000, 1785456000, 1788048000,
    )
    assert PyNoticeMessage.from_fields(notice.to_fields()) == notice
