"""单元测试：NTP 时间戳换算、报文编解码、四态判定、探测回包校验。

运行：pytest tests/test_units.py
"""
from __future__ import annotations

import socket
import threading
import time

import pytest

from common.ntp_proto import (
    MODE_CLIENT,
    MODE_SERVER,
    build_client_packet,
    build_server_packet,
    ntp_to_unix,
    parse_packet,
    unix_to_ntp,
)
from diag.app.probe import ProbeError, probe_source
from diag.app.verdict import Verdict, decide

# ------------------------------------------------------------ 时间戳换算


def test_ntp_timestamp_roundtrip():
    for ts in (0.0, 1_700_000_000.0, 1_700_000_000.123456, time.time()):
        assert abs(ntp_to_unix(unix_to_ntp(ts)) - ts) < 1e-6


def test_ntp_epoch_offset():
    # 1970-01-01 对应 NTP 秒 2208988800
    assert unix_to_ntp(0.0) >> 32 == 2_208_988_800


# ------------------------------------------------------------ 报文编解码


def test_client_packet_roundtrip():
    t1 = unix_to_ntp(time.time())
    pkt = parse_packet(build_client_packet(t1))
    assert pkt["mode"] == MODE_CLIENT
    assert pkt["version"] == 4
    assert pkt["transmit"] == t1


def test_server_packet_roundtrip():
    t1, t2, t3 = (unix_to_ntp(time.time() + d) for d in (0, 0.01, 0.02))
    pkt = parse_packet(build_server_packet(t1, t2, t3))
    assert pkt["mode"] == MODE_SERVER
    assert pkt["originate"] == t1
    assert pkt["receive"] == t2
    assert pkt["transmit"] == t3


def test_parse_rejects_short_packet():
    with pytest.raises(ValueError):
        parse_packet(b"\x00" * 10)


# ------------------------------------------------------------ 四态判定


def test_decide_ok_when_intersection_inside_tolerance():
    d = decide([(4.0, 6.0), (-9.0, 5.0)], tolerance_ms=10.0)  # 交集 [4,5]
    assert d.verdict is Verdict.OK
    assert d.intersection_ms == pytest.approx((4.0, 5.0))


def test_decide_need_when_intersection_fully_outside():
    assert decide([(95.0, 105.0), (90.0, 110.0)], 50.0).verdict is Verdict.NEED
    assert decide([(-105.0, -95.0), (-110.0, -90.0)], 50.0).verdict is Verdict.NEED


def test_decide_pending_when_crossing_boundary():
    assert decide([(35.0, 55.0), (30.0, 60.0)], 50.0).verdict is Verdict.PENDING
    assert decide([(-55.0, -35.0), (-60.0, -30.0)], 50.0).verdict is Verdict.PENDING
    # 交集比整个容差窗口还宽，两侧都跨
    assert decide([(-80.0, 80.0), (-90.0, 70.0)], 50.0).verdict is Verdict.PENDING


def test_decide_unknown_when_disjoint():
    d = decide([(95.0, 105.0), (-105.0, -95.0)], 50.0)
    assert d.verdict is Verdict.UNKNOWN
    assert d.intersection_ms is None


def test_decide_unknown_when_any_source_failed():
    assert decide([None, (1.0, 2.0)], 50.0).verdict is Verdict.UNKNOWN
    assert decide([(1.0, 2.0), None], 50.0).verdict is Verdict.UNKNOWN
    assert decide([None, None], 50.0).verdict is Verdict.UNKNOWN


def test_decide_boundary_touching_counts_as_inside():
    # 交集端点恰好贴到容差边界：视为“完全在容差内”
    assert decide([(-50.0, 50.0), (-50.0, 50.0)], 50.0).verdict is Verdict.OK


# ------------------------------------------------------------ 探测回包校验（真实 UDP，本机环回）


class FakeNtpServer(threading.Thread):
    """行为可编程的假 NTP 源，用于测试各种回包异常。"""

    def __init__(self, mode: str, offset_s: float = 0.0, delay_s: float = 0.0):
        super().__init__(daemon=True)
        self.mode = mode
        self.offset_s = offset_s
        self.delay_s = delay_s
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", 0))
        self.port = self.sock.getsockname()[1]

    def run(self):
        while True:
            try:
                data, addr = self.sock.recvfrom(2048)
            except OSError:
                return
            try:
                self._handle(data, addr)
            except OSError:
                return  # 测试结束 socket 已关闭

    def _handle(self, data: bytes, addr) -> None:  # noqa: ANN001
        if self.mode == "drop":
            return  # 不应答 -> 客户端超时
        if self.mode == "garbage":
            self.sock.sendto(b"\xde\xad\xbe\xef", addr)
            return
        if self.mode == "wrong_originate":
            req = parse_packet(data)
            t = unix_to_ntp(time.time() + self.offset_s)
            self.sock.sendto(build_server_packet(req["transmit"] + 1, t, t), addr)
            return
        if self.mode == "bad_stratum":
            req = parse_packet(data)
            t = unix_to_ntp(time.time() + self.offset_s)
            self.sock.sendto(build_server_packet(req["transmit"], t, t, stratum=0), addr)
            return
        if self.mode == "echo_client_mode":
            self.sock.sendto(data, addr)  # mode 仍是 client
            return
        # 正常模式：可带钟差与回包延迟；originate 回显客户端 transmit。
        # t3 先盖章再滞留，模拟回程网络时延（与真实源端实现一致）。
        req = parse_packet(data)
        t2 = time.time() + self.offset_s
        t3 = time.time() + self.offset_s
        if self.delay_s:
            time.sleep(self.delay_s)
        self.sock.sendto(
            build_server_packet(req["transmit"], unix_to_ntp(t2), unix_to_ntp(t3)),
            addr,
        )

    def stop(self):
        self.sock.close()


@pytest.fixture
def fake_server():
    holder = {}

    def make(mode="normal", **kw):
        srv = FakeNtpServer(mode, **kw)
        srv.start()
        holder["srv"] = srv
        return srv

    yield make
    if "srv" in holder:
        holder["srv"].stop()


def test_probe_normal_offset_and_interval(fake_server):
    srv = fake_server("normal", offset_s=0.120, delay_s=0.050)
    r = probe_source("127.0.0.1", srv.port, timeout_s=2.0)
    # 回包延迟全部落在回程路径：δ 不小于 50ms，点估值 θ 被拉低约 δ/2，
    # 但 NTP 误差界必须成立：|θ - 真实钟差| ≤ δ/2，且区间包含真实钟差
    assert r.delay_s >= 0.050 - 1e-3
    assert abs(r.offset_s - 0.120) <= r.delay_s / 2 + 0.005
    assert r.low_s <= 0.120 <= r.high_s
    assert r.t1 <= r.t4


def test_probe_timeout_when_no_reply(fake_server):
    srv = fake_server("drop")
    t0 = time.time()
    with pytest.raises(ProbeError, match="超时"):
        probe_source("127.0.0.1", srv.port, timeout_s=0.3)
    assert time.time() - t0 < 1.0


def test_probe_rejects_garbage(fake_server):
    srv = fake_server("garbage")
    with pytest.raises(ProbeError, match="无效回包"):
        probe_source("127.0.0.1", srv.port, timeout_s=1.0)


def test_probe_rejects_mismatched_originate(fake_server):
    srv = fake_server("wrong_originate")
    with pytest.raises(ProbeError, match="迟到/串包"):
        probe_source("127.0.0.1", srv.port, timeout_s=1.0)


def test_probe_rejects_unsynchronized_stratum(fake_server):
    srv = fake_server("bad_stratum")
    with pytest.raises(ProbeError, match="stratum"):
        probe_source("127.0.0.1", srv.port, timeout_s=1.0)


def test_probe_rejects_non_server_mode(fake_server):
    srv = fake_server("echo_client_mode")
    with pytest.raises(ProbeError, match="mode"):
        probe_source("127.0.0.1", srv.port, timeout_s=1.0)


def test_probe_late_reply_times_out(fake_server):
    # 回包延迟 0.6s 超过客户端 0.2s 超时 -> 按超时拒绝
    srv = fake_server("normal", delay_s=0.6)
    with pytest.raises(ProbeError, match="超时"):
        probe_source("127.0.0.1", srv.port, timeout_s=0.2)


def test_probe_concurrent_no_crosstalk(fake_server):
    """并发探测同一源：每轮结果都必须对应自己的请求（originate 校验）。"""
    srv = fake_server("normal", offset_s=0.050, delay_s=0.020)
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(
            lambda _: probe_source("127.0.0.1", srv.port, timeout_s=2.0), range(32)
        ))
    t1s = [r.t1 for r in results]
    assert len(set(t1s)) == len(t1s)  # 每轮请求各自独立
    for r in results:
        # 每轮都满足 NTP 误差界，说明没有任何一轮读到别人的回包
        assert abs(r.offset_s - 0.050) <= r.delay_s / 2 + 0.005
        assert r.low_s <= 0.050 <= r.high_s
