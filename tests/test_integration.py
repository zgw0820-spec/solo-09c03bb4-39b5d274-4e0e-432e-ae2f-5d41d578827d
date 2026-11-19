"""端到端集成测试：真实拉起两台 NTP 源 + 诊断服务，走完整 HTTP/UDP 链路。

运行：pytest tests/test_integration.py
（需要本机可绑定 18000/19001/19002 TCP 与 18123/18124 UDP 端口）
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

SRC1_UDP, SRC1_HTTP = 18123, 19001
SRC2_UDP, SRC2_HTTP = 18124, 19002
DIAG_HTTP = 18000
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


@pytest.fixture(scope="module")
def stack():
    env = os.environ.copy()
    procs = []
    for sid, udp, http in ((1, SRC1_UDP, SRC1_HTTP), (2, SRC2_UDP, SRC2_HTTP)):
        e = env | {
            "SOURCE_ID": f"ntp-source-{sid}", "UDP_PORT": str(udp),
            "HTTP_PORT": str(http), "INITIAL_OFFSET_MS": "0", "INITIAL_DELAY_MS": "10",
        }
        procs.append(subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "source.app.server:app",
             "--host", "127.0.0.1", "--port", str(http)],
            cwd=ROOT, env=e,
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        ))
    diag_env = env | {
        "NTP1_HOST": "127.0.0.1", "NTP1_UDP_PORT": str(SRC1_UDP),
        "NTP1_HTTP_URL": f"http://127.0.0.1:{SRC1_HTTP}",
        "NTP2_HOST": "127.0.0.1", "NTP2_UDP_PORT": str(SRC2_UDP),
        "NTP2_HTTP_URL": f"http://127.0.0.1:{SRC2_HTTP}",
    }
    procs.append(subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "diag.app.main:app",
         "--host", "127.0.0.1", "--port", str(DIAG_HTTP)],
        cwd=ROOT, env=diag_env,
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
    ))
    try:
        _wait_http(f"http://127.0.0.1:{SRC1_HTTP}/health")
        _wait_http(f"http://127.0.0.1:{SRC2_HTTP}/health")
        _wait_http(f"{BASE}/api/health")
        yield
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait(timeout=10)


def _set_source(sid: int, offset_ms: float, delay_ms: float) -> None:
    r = httpx.post(f"{BASE}/api/sources/{sid}/config",
                   json={"offset_ms": offset_ms, "reply_delay_ms": delay_ms}, timeout=10)
    r.raise_for_status()
    cur = r.json()["current"]
    assert cur["offset_ms"] == pytest.approx(offset_ms)
    assert cur["reply_delay_ms"] == pytest.approx(delay_ms)


def _diagnose(tolerance_ms: float, timeout_ms: int = 2000) -> dict:
    r = httpx.post(f"{BASE}/api/diagnose",
                   json={"tolerance_ms": tolerance_ms, "timeout_ms": timeout_ms},
                   timeout=timeout_ms / 1000 + 10)
    r.raise_for_status()
    return r.json()


# ------------------------------------------------------------ 场景


def test_web_page_served(stack):
    r = httpx.get(f"{BASE}/", timeout=5)
    assert r.status_code == 200
    assert "校时" in r.text and "容许偏差" in r.text


def test_sources_listing_and_config_roundtrip(stack):
    _set_source(1, 5.0, 10.0)
    _set_source(2, -8.0, 15.0)
    data = httpx.get(f"{BASE}/api/sources", timeout=10).json()
    by_id = {s["id"]: s for s in data["sources"]}
    assert by_id[1]["reachable"] and by_id[2]["reachable"]
    assert by_id[1]["offset_ms"] == pytest.approx(5.0)
    assert by_id[2]["reply_delay_ms"] == pytest.approx(15.0)


def test_diagnose_ok_within_tolerance(stack):
    _set_source(1, 5.0, 10.0)
    _set_source(2, 3.0, 12.0)
    r = _diagnose(50.0)
    assert r["verdict"] == "no_calibration_needed", r
    s1, s2 = r["sources"]
    assert s1["ok"] and s2["ok"]
    # 四时间戳与派生量合理：δ 不小于设定回包延迟，且满足 NTP 误差界
    assert s1["delay_ms"] >= 10.0 - 1.0
    assert s2["delay_ms"] >= 12.0 - 1.0
    assert abs(s1["offset_ms"] - 5.0) <= s1["delay_ms"] / 2 + 2.0
    assert abs(s2["offset_ms"] - 3.0) <= s2["delay_ms"] / 2 + 2.0
    # 区间包含真实钟差（±2ms 时钟分辨率裕量）
    assert s1["interval_ms"][0] - 2.0 <= 5.0 <= s1["interval_ms"][1] + 2.0
    assert s2["interval_ms"][0] - 2.0 <= 3.0 <= s2["interval_ms"][1] + 2.0
    lo, hi = r["intersection_ms"]
    assert lo <= hi and hi <= 50.0 and lo >= -50.0


def test_diagnose_need_calibration(stack):
    _set_source(1, 200.0, 10.0)
    _set_source(2, 200.0, 10.0)
    r = _diagnose(50.0)
    assert r["verdict"] == "calibration_needed", r
    lo, hi = r["intersection_ms"]
    assert lo > 50.0


def test_diagnose_unknown_when_intervals_disjoint(stack):
    _set_source(1, 100.0, 5.0)
    _set_source(2, -100.0, 5.0)
    r = _diagnose(50.0)
    assert r["verdict"] == "undetermined", r
    assert r["intersection_ms"] is None
    assert "不相交" in r["reason"]


def test_diagnose_unknown_when_source_timeout(stack):
    _set_source(1, 3.0, 10.0)
    _set_source(2, 3.0, 6000.0)  # 回包延迟超过探测超时
    r = _diagnose(50.0, timeout_ms=1200)
    assert r["verdict"] == "undetermined", r
    assert r["sources"][0]["ok"] is True
    assert r["sources"][1]["ok"] is False
    assert "超时" in r["sources"][1]["error"]
    _set_source(2, 3.0, 10.0)  # 恢复，避免影响后续用例


def test_diagnose_pending_when_crossing_boundary(stack):
    # 区间约 [offset-δ, offset] = [35, 55]，跨越 +50 容差边界
    _set_source(1, 55.0, 20.0)
    _set_source(2, 55.0, 20.0)
    r = _diagnose(50.0)
    assert r["verdict"] == "pending_review", r
    lo, hi = r["intersection_ms"]
    assert lo < 50.0 < hi


def test_concurrent_diagnoses_no_crosstalk(stack):
    """12 路并发诊断：全部探测成功、各轮 t1 互不重复、结果各自自洽。

    负载尖峰会拉大实测 δ，把判定从“无需校时”推向“待核”——这是系统的
    正确行为；本用例关注的是并发不串包：每轮区间都必须包含真实钟差，
    且上报的交集/判定与各源区间自洽。
    """
    _set_source(1, 5.0, 15.0)
    _set_source(2, 3.0, 12.0)
    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(lambda _: _diagnose(50.0), range(12)))
    t1s = [s["timestamps"]["t1"] for r in results for s in r["sources"]]
    assert len(set(t1s)) == len(t1s)  # 24 次探测各自独立
    for r in results:
        s1, s2 = r["sources"]
        assert s1["ok"] and s2["ok"], r
        # 各轮区间都罩住对应源的真实钟差（未被别轮回包污染）
        assert s1["interval_ms"][0] - 2.0 <= 5.0 <= s1["interval_ms"][1] + 2.0
        assert s2["interval_ms"][0] - 2.0 <= 3.0 <= s2["interval_ms"][1] + 2.0
        # 上报交集 = 两源区间之交
        lo = max(s1["interval_ms"][0], s2["interval_ms"][0])
        hi = min(s1["interval_ms"][1], s2["interval_ms"][1])
        assert r["intersection_ms"] == pytest.approx([lo, hi])
        # 钟差仅几 ms：判定只能是“无需校时”或（δ 尖峰时）“待核”
        assert r["verdict"] in ("no_calibration_needed", "pending_review")


def test_invalid_source_id_rejected(stack):
    r = httpx.post(f"{BASE}/api/sources/3/config", json={"offset_ms": 1}, timeout=5)
    assert r.status_code == 404


def test_invalid_diagnose_params_rejected(stack):
    r = httpx.post(f"{BASE}/api/diagnose", json={"tolerance_ms": -5}, timeout=5)
    assert r.status_code == 422
