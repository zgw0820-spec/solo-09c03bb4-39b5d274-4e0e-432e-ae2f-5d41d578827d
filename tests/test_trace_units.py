"""链路追踪单元测试：请求标识编解码、源端记录表、诊断端状态判定。

运行：pytest tests/test_trace_units.py
"""
from __future__ import annotations

import pytest

from common.ntp_proto import (
    NTP_PACKET_SIZE,
    TRACE_EXT_LEN,
    build_client_packet,
    extract_trace_id,
    parse_packet,
    unix_to_ntp,
)
from diag.app.link import ProbeOutcome, analyze_link
from diag.app.probe import ProbeError, probe_source
from source.app.trace_store import TraceStore

TID = "0123456789abcdef"
OTHER_TID = "fedcba9876543210"


# ------------------------------------------------------------ 请求标识


def test_trace_id_roundtrip_in_packet():
    t1_ntp = unix_to_ntp(1_700_000_000.123456)
    packet = build_client_packet(t1_ntp, trace_id=TID)
    assert len(packet) == NTP_PACKET_SIZE + TRACE_EXT_LEN
    assert extract_trace_id(packet) == TID
    # 标准 48 字节头解析不受扩展影响
    assert parse_packet(packet)["transmit"] == t1_ntp


def test_packet_without_trace_has_no_trace_id():
    assert extract_trace_id(build_client_packet(123)) is None
    assert extract_trace_id(b"\x00" * (NTP_PACKET_SIZE + 5)) is None


def test_bad_trace_id_rejected():
    with pytest.raises(ValueError):
        build_client_packet(1, trace_id="NOT-HEX")
    with pytest.raises(ValueError):
        build_client_packet(1, trace_id="abc")


def test_extract_ignores_garbage_extension():
    packet = build_client_packet(1) + b"\xff" * TRACE_EXT_LEN
    assert extract_trace_id(packet) is None


# ------------------------------------------------------------ 源端记录表


class FakeClock:
    def __init__(self) -> None:
        self.mono = 1000.0

    def advance(self, seconds: float) -> None:
        self.mono += seconds

    def __call__(self) -> float:
        return self.mono


@pytest.fixture
def clock():
    return FakeClock()


def _store(clock, **kw):
    return TraceStore(
        "ntp-source-1", clock=clock, wall_clock=lambda: 1_700_000_000.0, **kw
    )


def test_store_received_then_sent(clock):
    store = _store(clock)
    store.record_received(TID, 111, 222)
    q = store.query(TID)
    assert q["status"] == "ok"
    assert len(q["records"]) == 1
    assert q["records"][0]["state"] == "received"
    assert q["records"][0]["sent_at"] is None

    clock.advance(0.05)
    store.record_sent(TID, 111, 333)
    rec = store.query(TID)["records"][0]
    assert rec["state"] == "sent"
    assert rec["transmit_ntp"] == 333
    assert rec["sent_at"] is not None


def test_store_not_found(clock):
    q = _store(clock).query(TID)
    assert q["status"] == "not_found" and q["records"] == []


def test_store_expires_but_still_reports_expired(clock):
    store = _store(clock, ttl_s=10, retain_expired_s=20)
    store.record_received(TID, 111, 222)
    clock.advance(11)
    q = store.query(TID)
    assert q["status"] == "expired"
    assert len(q["records"]) == 1  # 仍能看到“曾收到”，但不作为本轮证据


def test_store_purges_after_retention(clock):
    store = _store(clock, ttl_s=10, retain_expired_s=5)
    store.record_received(TID, 111, 222)
    clock.advance(16)
    assert store.query(TID)["status"] == "not_found"


def test_store_keeps_same_id_different_originate_apart(clock):
    store = _store(clock)
    store.record_received(TID, 111, 222)
    store.record_sent(TID, 111, 333)
    store.record_received(TID, 444, 555)  # 同标识的另一轮（异常/复用）
    origins = {r["originate"] for r in store.query(TID)["records"]}
    assert origins == {111, 444}


def test_store_ignores_sent_for_unknown_record(clock):
    store = _store(clock)
    store.record_sent(TID, 111, 333)  # 没收到过就发包：不凭空补记录
    assert store.query(TID)["status"] == "not_found"


def test_store_capacity_is_bounded(clock):
    store = _store(clock, max_entries=3)
    for i in range(5):
        store.record_received(f"{i:016x}", i, i)
    assert len(store._entries) == 3
    assert store.query(f"{0:016x}")["status"] == "not_found"
    assert store.query(f"{4:016x}")["status"] == "ok"


def test_store_boot_id_stable_within_instance(clock):
    store = _store(clock)
    assert store.query(TID)["boot_id"] == store.boot_id
    assert _store(clock).boot_id != store.boot_id


# ------------------------------------------------------------ 状态判定

BOOT = "boot-1"
T1_NTP = 9_876_543_210


def _ok_outcome(**kw):
    base = dict(ok=True, trace_id=TID, t1_ntp=T1_NTP, t1=100.0, t4=100.1)
    base.update(kw)
    return ProbeOutcome(**base)


def _err_outcome(kind="timeout", **kw):
    base = dict(
        ok=False, error_kind=kind, error_text="x", trace_id=TID,
        t1_ntp=T1_NTP, t1=100.0, t4=None, error=ProbeError("x", kind=kind),
    )
    base.update(kw)
    return ProbeOutcome(**base)


def _query(records=None, status="ok", boot_after=BOOT):
    return {
        "source": "ntp-source-1", "boot_id": boot_after, "trace_id": TID,
        "status": status, "records": records or [],
    }


def _rec(originate=T1_NTP, sent=True):
    return {
        "originate": originate, "receive_ntp": 1, "transmit_ntp": 2 if sent else None,
        "received_at": 100.05, "sent_at": 100.06 if sent else None,
        "state": "sent" if sent else "received",
    }


def test_source_no_request():
    r = analyze_link(_err_outcome(), _query([], status="not_found"), boot_before=BOOT)
    assert r.state.code == "source_no_request" and r.state.conclusive


def test_source_received_not_sent():
    r = analyze_link(_err_outcome(), _query([_rec(sent=False)]), boot_before=BOOT)
    assert r.state.code == "source_received_not_sent" and r.state.conclusive


def test_source_sent_diag_not_received():
    r = analyze_link(_err_outcome("timeout"), _query([_rec()]), boot_before=BOOT)
    assert r.state.code == "source_sent_diag_not_received" and r.state.conclusive


def test_source_sent_diag_late_and_invalid():
    late = analyze_link(_err_outcome("late_reply"), _query([_rec()]), boot_before=BOOT)
    assert late.state.code == "source_sent_diag_late"
    bad = analyze_link(_err_outcome("invalid_reply"), _query([_rec()]), boot_before=BOOT)
    assert bad.state.code == "source_sent_diag_invalid_reply"


def test_source_sent_diag_received():
    r = analyze_link(_ok_outcome(), _query([_rec()]), boot_before=BOOT)
    assert r.state.code == "source_sent_diag_received" and r.state.conclusive
    assert r.timeline["source_sent_at"] == 100.06


# ---- 证据不足的各种情形，均不得给出“网络丢包”结论


def test_control_unreachable_is_insufficient():
    r = analyze_link(
        _err_outcome(), None, trace_error="connect refused", boot_before=BOOT,
    )
    assert r.state.code == "insufficient_evidence"
    assert not r.state.conclusive and r.control_reachable is False
    assert "不能据此断定网络丢包" in r.state.detail


def test_record_expired_is_insufficient():
    r = analyze_link(_err_outcome(), _query([_rec()], status="expired"), boot_before=BOOT)
    assert r.state.code == "insufficient_evidence" and not r.state.conclusive


def test_source_restarted_is_insufficient():
    r = analyze_link(_err_outcome(), _query([_rec()], boot_after="boot-2"), boot_before=BOOT)
    assert r.state.code == "insufficient_evidence"
    assert "重启" in r.state.detail


def test_missing_boot_before_is_insufficient():
    r = analyze_link(_err_outcome(), _query([_rec()]), boot_before=None)
    assert r.state.code == "insufficient_evidence" and not r.state.conclusive


def test_record_for_other_originate_not_borrowed():
    # 同一标识下只有别轮 t1 的记录：绝不借用
    r = analyze_link(_err_outcome(), _query([_rec(originate=T1_NTP + 1)]), boot_before=BOOT)
    assert r.state.code == "insufficient_evidence" and not r.state.conclusive


def test_unexpected_probe_without_trace_id_is_insufficient():
    outcome = ProbeOutcome(ok=False, error_kind="unexpected", error=RuntimeError("boom"))
    r = analyze_link(outcome, _query([_rec()]), boot_before=BOOT)
    assert r.state.code == "insufficient_evidence" and not r.state.conclusive


def test_ok_but_not_found_record_is_insufficient():
    r = analyze_link(_ok_outcome(), _query([], status="not_found"), boot_before=BOOT)
    assert r.state.code == "insufficient_evidence" and not r.state.conclusive


# ------------------------------------------------------------ 探测携带标识


def test_probe_result_carries_trace_id_and_t1_ntp():
    import socket
    import threading
    import time as _time

    from common.ntp_proto import build_server_packet

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    seen = {}

    def serve():
        data, addr = sock.recvfrom(2048)
        seen["trace_id"] = extract_trace_id(data)
        req = parse_packet(data)
        t = unix_to_ntp(_time.time())
        sock.sendto(build_server_packet(req["transmit"], t, t), addr)

    th = threading.Thread(target=serve, daemon=True)
    th.start()
    try:
        r = probe_source("127.0.0.1", port, timeout_s=2.0)
    finally:
        th.join(timeout=2)
        sock.close()
    assert r.trace_id == seen["trace_id"]
    assert len(r.trace_id) == 16
    assert r.t1_ntp == unix_to_ntp(r.t1)
