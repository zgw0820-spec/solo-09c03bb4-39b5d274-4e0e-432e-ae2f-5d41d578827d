"""链路追踪端到端测试：真实拉起源 + 诊断服务，验证各环节状态与证据边界。

运行：pytest tests/test_trace_integration.py
（需要本机可绑定 18010/19011/19012 TCP 与 18133/18134 UDP 端口）
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent

SRC1_UDP, SRC1_HTTP = 18133, 19011
SRC2_UDP, SRC2_HTTP = 18134, 19012
DIAG_HTTP = 18010
BASE = f"http://127.0.0.1:{DIAG_HTTP}"


def _wait_http(url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=1.0).status_code == 200:
                return
        except httpx.HTTPError:
            time.sleep(0.2)
    raise RuntimeError(f"服务未就绪：{url}")


def _start_source(sid: int, udp: int, http: int, *, ttl_s: float = 120,
                  offset_ms: float = 0.0, delay_ms: float = 10.0,
                  extra_env: dict | None = None) -> subprocess.Popen:
    env = os.environ.copy() | {
        "SOURCE_ID": f"ntp-source-{sid}", "UDP_PORT": str(udp),
        "HTTP_PORT": str(http), "INITIAL_OFFSET_MS": str(offset_ms),
        "INITIAL_DELAY_MS": str(delay_ms), "TRACE_TTL_S": str(ttl_s),
    } | (extra_env or {})
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "source.app.server:app",
         "--host", "127.0.0.1", "--port", str(http)],
        cwd=ROOT, env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
    )


def _start_diag(
    src1_udp: int = SRC1_UDP, src2_udp: int = SRC2_UDP,
    src1_http: int = SRC1_HTTP, src2_http: int = SRC2_HTTP,
    port: int = DIAG_HTTP,
) -> subprocess.Popen:
    env = os.environ.copy() | {
        "NTP1_HOST": "127.0.0.1", "NTP1_UDP_PORT": str(src1_udp),
        "NTP1_HTTP_URL": f"http://127.0.0.1:{src1_http}",
        "NTP2_HOST": "127.0.0.1", "NTP2_UDP_PORT": str(src2_udp),
        "NTP2_HTTP_URL": f"http://127.0.0.1:{src2_http}",
    }
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "diag.app.main:app",
         "--host", "127.0.0.1", "--port", str(port)],
        cwd=ROOT, env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
    )


def _terminate(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()


@pytest.fixture(scope="module")
def stack():
    procs = [
        _start_source(1, SRC1_UDP, SRC1_HTTP),
        _start_source(2, SRC2_UDP, SRC2_HTTP),
        _start_diag(),
    ]
    try:
        _wait_http(f"http://127.0.0.1:{SRC1_HTTP}/health")
        _wait_http(f"http://127.0.0.1:{SRC2_HTTP}/health")
        _wait_http(f"{BASE}/api/health")
        yield procs
    finally:
        _terminate(procs)


def _set_source(sid: int, offset_ms: float, delay_ms: float) -> None:
    r = httpx.post(f"{BASE}/api/sources/{sid}/config",
                   json={"offset_ms": offset_ms, "reply_delay_ms": delay_ms}, timeout=10)
    r.raise_for_status()


def _diagnose(tolerance_ms: float = 50.0, timeout_ms: int = 2000) -> dict:
    r = httpx.post(f"{BASE}/api/diagnose",
                   json={"tolerance_ms": tolerance_ms, "timeout_ms": timeout_ms},
                   timeout=timeout_ms / 1000 + 15)
    r.raise_for_status()
    return r.json()


def _link(result: dict, sid: int) -> dict:
    return {link["source_id"]: link for link in result["links"]}[sid]


# ------------------------------------------------------------ 正常链路


def test_all_sources_sent_and_received(stack):
    _set_source(1, 5.0, 10.0)
    _set_source(2, 3.0, 12.0)
    r = _diagnose()
    assert r["verdict"] == "no_calibration_needed", r
    for sid in (1, 2):
        link = _link(r, sid)
        assert link["state"] == "source_sent_diag_received", link
        assert link["conclusive"] is True
        assert link["trace_id"] and len(link["trace_id"]) == 16
        assert link["record"]["sent_at"] is not None
        assert link["record"]["originate"] == link["record"]["originate"]
        assert link["boot_before"] == link["boot_after"]
        assert link["timeline"]["request_sent_at"] is not None
        assert link["timeline"]["source_sent_at"] is not None


def test_trace_ids_unique_per_round(stack):
    ids = set()
    for _ in range(3):
        r = _diagnose()
        for link in r["links"]:
            ids.add(link["trace_id"])
    assert len(ids) == 6


# ------------------------------------------------------------ 源已收包但尚未发出


def test_source_received_not_sent(stack):
    # 回包延迟 5s 远大于查询窗口：源已收包，但诊断超时后立刻查记录时尚未发包
    _set_source(1, 5.0, 5000.0)
    _set_source(2, 5.0, 10.0)
    try:
        r = _diagnose(timeout_ms=400)
        link = _link(r, 1)
        assert link["state"] == "source_received_not_sent", link
        assert link["conclusive"] is True
        assert link["record"]["sent_at"] is None
        assert link["probe_error_kind"] == "timeout"
    finally:
        _set_source(1, 5.0, 10.0)


def test_source_sent_diag_not_received(stack):
    # 回包延迟略大于探测超时：诊断超时，随后源端仍会照常发包
    _set_source(1, 5.0, 800.0)
    _set_source(2, 5.0, 10.0)
    try:
        r = _diagnose(timeout_ms=300)
        link = _link(r, 1)
        assert link["probe_error_kind"] == "timeout"
        assert link["state"] in ("source_sent_diag_not_received",
                                 "source_received_not_sent"), link
        # 等回包延迟过去后按本轮标识重查：源已发包、诊断机超时未收
        deadline = time.time() + 5
        while time.time() < deadline:
            again = httpx.post(f"{BASE}/api/trace",
                               json={"round_id": r["round_id"], "source_id": 1},
                               timeout=10).json()
            if again["state"] == "source_sent_diag_not_received":
                break
            time.sleep(0.1)
        assert again["state"] == "source_sent_diag_not_received", again
        assert again["conclusive"] is True
        assert again["trace_id"] == link["trace_id"]  # 仍是本轮标识
        assert again["timeline"]["source_sent_at"] is not None
        # 该迟到回包不会改变本轮判定/其他轮次
        r2 = _diagnose()
        assert _link(r2, 1)["state"] == "source_sent_diag_received"
    finally:
        _set_source(1, 5.0, 10.0)


def test_retrace_same_round_only_uses_own_record(stack):
    _set_source(1, 5.0, 10.0)
    _set_source(2, 5.0, 10.0)
    r = _diagnose()
    sid = 1
    trace_id = _link(r, sid)["trace_id"]
    again = httpx.post(f"{BASE}/api/trace",
                       json={"round_id": r["round_id"], "source_id": sid}, timeout=10)
    again.raise_for_status()
    data = again.json()
    assert data["trace_id"] == trace_id  # 用的仍是本轮的标识
    assert data["state"] == "source_sent_diag_received", data


def test_retrace_unknown_round_rejected(stack):
    r = httpx.post(f"{BASE}/api/trace",
                   json={"round_id": "does-not-exist", "source_id": 1}, timeout=5)
    assert r.status_code == 404


def test_retrace_unknown_source_rejected(stack):
    r = _diagnose()
    resp = httpx.post(f"{BASE}/api/trace",
                      json={"round_id": r["round_id"], "source_id": 9}, timeout=5)
    assert resp.status_code == 404


# ------------------------------------------------------------ 并发隔离


def test_concurrent_rounds_do_not_share_trace_records(stack):
    _set_source(1, 5.0, 15.0)
    _set_source(2, 3.0, 12.0)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: _diagnose(), range(8)))
    trace_ids = [link["trace_id"] for r in results for link in r["links"]]
    assert len(set(trace_ids)) == len(trace_ids)  # 每轮每源独立标识
    for r in results:
        for link in r["links"]:
            assert link["state"] == "source_sent_diag_received", r
            # 每条记录都是本轮标识对应的那一条
            assert link["record"] is not None
            assert link["record"]["originate"] == link["record"]["originate"]
            assert link["record"]["sent_at"] is not None


# ------------------------------------------------------------ 证据边界（独立栈）


def _control_unreachable_stack() -> list[subprocess.Popen]:
    return [
        _start_source(1, 18143, 19021),
        _start_source(2, 18144, 19022),
        # 源 1 的 UDP 正常，但诊断端指向无人监听的 19099 控制口
        _start_diag(18143, 18144, 19099, 19022, port=18011),
    ]


def test_control_unreachable_reports_insufficient():
    """源控制口不可达：UDP 正常、只把诊断端指向关闭的控制口。"""
    base = "http://127.0.0.1:18011"
    procs = _control_unreachable_stack()
    try:
        _wait_http("http://127.0.0.1:19021/health")
        _wait_http("http://127.0.0.1:19022/health")
        _wait_http(f"{base}/api/health")
        r = httpx.post(f"{base}/api/diagnose",
                       json={"tolerance_ms": 50, "timeout_ms": 1500}, timeout=20).json()
        link = _link(r, 1)
        assert link["control_reachable"] is False
        assert link["state"] == "insufficient_evidence", link
        assert link["conclusive"] is False
        assert "不能据此断定网络丢包" in link["detail"]
        # 控制口不可达不影响真实 UDP 探测本身与判定
        assert r["sources"][0]["ok"] is True
    finally:
        _terminate(procs)


def test_source_restart_during_probe_is_insufficient():
    """源在探测期间重启：boot_id 变化后只报证据不足。"""
    procs = [
        _start_source(1, 18153, 19031),
        _start_source(2, 18154, 19032),
        _start_diag(18153, 18154, 19031, 19032, port=18012),
    ]
    try:
        _wait_http("http://127.0.0.1:19031/health")
        _wait_http("http://127.0.0.1:19032/health")
        _wait_http("http://127.0.0.1:18012/api/health")
        # 探测前 boot_id
        boot_before = httpx.get("http://127.0.0.1:19031/health", timeout=5).json()["boot_id"]

        # 让探测耗时较长，期间重启源 1
        httpx.post("http://127.0.0.1:19031/config",
                   json={"reply_delay_ms": 400}, timeout=5).raise_for_status()

        import threading
        def restart_later():
            time.sleep(0.15)
            procs[0].terminate()
            procs[0].wait(timeout=10)

        th = threading.Thread(target=restart_later)
        th.start()
        r = httpx.post("http://127.0.0.1:18012/api/diagnose",
                       json={"tolerance_ms": 50, "timeout_ms": 1500}, timeout=25).json()
        th.join()
        link = _link(r, 1)
        # 源 1 在探测期间停止：查询控制口失败，只能报证据不足
        assert link["state"] == "insufficient_evidence", link
        assert link["conclusive"] is False
        assert "不能据此断定网络丢包" in link["detail"]
        # 探测前读数要么是我们先前取的启动标识，要么因源已被终止而取不到
        assert link["boot_before"] in (boot_before, None)
    finally:
        _terminate(procs)


def test_expired_trace_records_report_insufficient():
    """记录 TTL 极短：探测后记录过期，只报证据不足。"""
    procs = [
        _start_source(1, 18163, 19041, ttl_s=0.1, delay_ms=3000.0),
        _start_source(2, 18164, 19042),
        _start_diag(18163, 18164, 19041, 19042, port=18013),
    ]
    try:
        _wait_http("http://127.0.0.1:19041/health")
        _wait_http("http://127.0.0.1:19042/health")
        _wait_http("http://127.0.0.1:18013/api/health")
        # 探测超时 300ms；源 1 回包延迟 3s，收包后记录在 0.1s 即过期，
        # 探测返回后再查询时已不能作为本轮证据
        r = httpx.post("http://127.0.0.1:18013/api/diagnose",
                       json={"tolerance_ms": 50, "timeout_ms": 300}, timeout=20).json()
        link = _link(r, 1)
        assert link["probe_error_kind"] == "timeout"
        assert link["state"] == "insufficient_evidence", link
        assert link["conclusive"] is False
        assert link["record_status"] in ("expired", "not_found")
    finally:
        _terminate(procs)

    # 记录过期后，同轮重查也只报证据不足
    procs = [
        _start_source(1, 18173, 19051, ttl_s=0.3),
        _start_source(2, 18174, 19052),
        _start_diag(18173, 18174, 19051, 19052, port=18014),
    ]
    try:
        _wait_http("http://127.0.0.1:19051/health")
        _wait_http("http://127.0.0.1:19052/health")
        _wait_http("http://127.0.0.1:18014/api/health")
        r = httpx.post("http://127.0.0.1:18014/api/diagnose",
                       json={"tolerance_ms": 50, "timeout_ms": 1500}, timeout=20).json()
        time.sleep(0.5)  # 让记录过期
        resp = httpx.post("http://127.0.0.1:18014/api/trace",
                          json={"round_id": r["round_id"], "source_id": 1}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        assert data["state"] == "insufficient_evidence", data
        assert data["record_status"] == "expired"
        assert data["conclusive"] is False
    finally:
        _terminate(procs)
