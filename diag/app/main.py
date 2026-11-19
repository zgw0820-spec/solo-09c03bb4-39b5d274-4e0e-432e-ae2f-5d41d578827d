"""诊断服务：操作网页 + API。

- GET  /                       操作网页
- GET  /api/sources            两台时间源的当前配置（实时取自源端控制口）
- POST /api/sources/{id}/config 调整指定源的钟差 / 回包延迟
- POST /api/diagnose           发起一轮真实 UDP 探测并给出四态判定
- POST /api/trace              针对指定时间源再查询一次本轮请求的收发记录
                               （仅限本进程内仍持有的追踪结果）

并发说明：每次 /api/diagnose 为每个源各开一个独立 UDP socket 探测
（见 probe.py），请求之间无任何共享状态，可安全并发。链路追踪记录按
“每轮、每源各自的请求标识”查询，绝不跨轮复用。
"""
from __future__ import annotations

import asyncio
import os
import secrets
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .link import LinkReport, ProbeOutcome, analyze_link
from .probe import ProbeError, ProbeResult, probe_source
from .verdict import VERDICT_TEXT, decide

# ---------------------------------------------------------------- 源注册表


@dataclass(frozen=True)
class SourceEndpoint:
    sid: int
    name: str
    udp_host: str
    udp_port: int
    http_url: str  # 源端控制口，用于读取/调整 钟差与回包延迟


def _build_sources() -> dict[int, SourceEndpoint]:
    return {
        1: SourceEndpoint(
            sid=1,
            name=os.getenv("NTP1_NAME", "ntp-source-1"),
            udp_host=os.getenv("NTP1_HOST", "127.0.0.1"),
            udp_port=int(os.getenv("NTP1_UDP_PORT", "123")),
            http_url=os.getenv("NTP1_HTTP_URL", "http://127.0.0.1:9001"),
        ),
        2: SourceEndpoint(
            sid=2,
            name=os.getenv("NTP2_NAME", "ntp-source-2"),
            udp_host=os.getenv("NTP2_HOST", "127.0.0.1"),
            udp_port=int(os.getenv("NTP2_UDP_PORT", "123")),
            http_url=os.getenv("NTP2_HTTP_URL", "http://127.0.0.1:9002"),
        ),
    }


SOURCES = _build_sources()

CONFIG_HTTP_TIMEOUT = 5.0
TRACE_QUERY_TIMEOUT = 2.0
_STATIC = Path(__file__).resolve().parent / "static"

# 每轮、每源的追踪结果暂存，供网页对超时源“再次查询”。有界且有 TTL，
# 键为诊断轮次 id + 源 id，轮与轮之间互不干扰。
TRACE_CACHE_TTL_S = 300.0
TRACE_CACHE_MAX_ROUNDS = 256
_TRACE_CACHE: OrderedDict[str, dict] = OrderedDict()  # round_id -> {sid: report}

app = FastAPI(title="机房设备校时前诊断", version="1.0.0")


# ---------------------------------------------------------------- 请求模型


class SourceConfigUpdate(BaseModel):
    offset_ms: float | None = Field(
        default=None, ge=-3_600_000, le=3_600_000,
        description="源钟相对真实时间的钟差（毫秒）",
    )
    reply_delay_ms: float | None = Field(
        default=None, ge=0, le=60_000,
        description="源端回包延迟（毫秒）",
    )


class DiagnoseRequest(BaseModel):
    tolerance_ms: float = Field(gt=0, le=600_000, description="容许偏差（毫秒）")
    timeout_ms: int = Field(default=2000, ge=200, le=30_000, description="单源探测超时（毫秒）")


class TraceQueryRequest(BaseModel):
    round_id: str = Field(min_length=1, max_length=64, description="诊断轮次 id")
    source_id: int = Field(description="时间源 id（1/2）")


# ---------------------------------------------------------------- 网页


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(_STATIC / "index.html")


# ---------------------------------------------------------------- 源配置


async def _fetch_source_config(src: SourceEndpoint) -> dict:
    try:
        async with httpx.AsyncClient(timeout=CONFIG_HTTP_TIMEOUT) as client:
            resp = await client.get(f"{src.http_url}/config")
            resp.raise_for_status()
            cfg = resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        return {
            "id": src.sid, "name": src.name,
            "udp": f"{src.udp_host}:{src.udp_port}",
            "reachable": False, "error": f"控制口不可达：{exc}",
        }
    return {
        "id": src.sid, "name": src.name,
        "udp": f"{src.udp_host}:{src.udp_port}",
        "reachable": True,
        "offset_ms": cfg.get("offset_ms"),
        "reply_delay_ms": cfg.get("reply_delay_ms"),
    }


@app.get("/api/sources")
async def list_sources() -> dict:
    """并行读取两源当前配置。"""
    configs = await asyncio.gather(*(_fetch_source_config(s) for s in SOURCES.values()))
    return {"sources": list(configs)}


@app.post("/api/sources/{sid}/config")
async def update_source_config(sid: int, update: SourceConfigUpdate) -> dict:
    src = SOURCES.get(sid)
    if src is None:
        raise HTTPException(status_code=404, detail=f"未知源 id={sid}，仅支持 1/2")
    payload = {k: v for k, v in update.model_dump().items() if v is not None}
    if not payload:
        raise HTTPException(status_code=400, detail="offset_ms 与 reply_delay_ms 至少给一项")
    try:
        async with httpx.AsyncClient(timeout=CONFIG_HTTP_TIMEOUT) as client:
            resp = await client.post(f"{src.http_url}/config", json=payload)
            resp.raise_for_status()
            cfg = resp.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"源 {sid} 拒绝配置：{exc.response.text}") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"源 {sid} 控制口不可达：{exc}") from exc
    return {"id": src.sid, "name": src.name, "applied": payload, "current": cfg}


# ---------------------------------------------------------------- 诊断


def _source_report_ok(src: SourceEndpoint, res: ProbeResult) -> dict:
    return {
        "id": src.sid, "name": src.name, "ok": True,
        "timestamps": {
            "t1": res.t1, "t2": res.t2, "t3": res.t3, "t4": res.t4,
        },
        "offset_ms": res.offset_s * 1000.0,
        "delay_ms": res.delay_s * 1000.0,
        "interval_ms": [res.low_s * 1000.0, res.high_s * 1000.0],
    }


def _source_report_err(src: SourceEndpoint, err: Exception) -> dict:
    report = {"id": src.sid, "name": src.name, "ok": False, "error": str(err)}
    if isinstance(err, ProbeError):
        report["error_kind"] = err.kind
    return report


# ---------------------------------------------------------------- 链路追踪


async def _fetch_boot_id(src: SourceEndpoint) -> str | None:
    """探测前读取源启动标识；控制口不可达时返回 None（不阻断探测）。"""
    try:
        async with httpx.AsyncClient(timeout=TRACE_QUERY_TIMEOUT) as client:
            resp = await client.get(f"{src.http_url}/health")
            resp.raise_for_status()
            boot = resp.json().get("boot_id")
    except (httpx.HTTPError, ValueError, AttributeError):
        return None
    return boot if isinstance(boot, str) and boot else None


async def _fetch_trace(src: SourceEndpoint, trace_id: str) -> tuple[dict | None, str | None]:
    """按本轮请求标识查询源端收发记录，返回 (记录, 错误说明)。"""
    try:
        async with httpx.AsyncClient(timeout=TRACE_QUERY_TIMEOUT) as client:
            resp = await client.get(f"{src.http_url}/trace/{trace_id}")
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        return None, f"HTTP {exc.response.status_code}：{exc.response.text[:200]}"
    except (httpx.HTTPError, ValueError) as exc:
        return None, f"控制口查询失败：{exc}"
    if not isinstance(data, dict):
        return None, "控制口返回格式异常"
    return data, None


def _outcome_of(res: ProbeResult | ProbeError | BaseException) -> ProbeOutcome:
    if isinstance(res, ProbeResult):
        return ProbeOutcome(
            ok=True, trace_id=res.trace_id, t1_ntp=res.t1_ntp, t1=res.t1, t4=res.t4,
        )
    if isinstance(res, ProbeError):
        return ProbeOutcome(
            ok=False, error_kind=res.kind, error_text=str(res),
            trace_id=res.trace_id, t1_ntp=res.t1_ntp, error=res,
        )
    return ProbeOutcome(ok=False, error_kind="unexpected", error_text=str(res), error=res)


def _outcome_payload(outcome: ProbeOutcome) -> dict:
    return {
        "ok": outcome.ok,
        "error_kind": outcome.error_kind,
        "error_text": outcome.error_text,
        "t1_ntp": outcome.t1_ntp,
        "t1": outcome.t1,
        "t4": outcome.t4,
    }


def _link_payload(src: SourceEndpoint, report: LinkReport, round_id: str, outcome: ProbeOutcome) -> dict:
    return {
        "round_id": round_id,
        "source_id": src.sid,
        "state": report.state.code,
        "label": report.state.label,
        "detail": report.state.detail,
        "conclusive": report.state.conclusive,
        "trace_id": report.trace_id,
        "probe_ok": outcome.ok,
        "probe_error_kind": outcome.error_kind,
        "control_reachable": report.control_reachable,
        "record_status": report.record_status,
        "boot_before": report.boot_before,
        "boot_after": report.boot_after,
        "record": report.record,
        "records_seen": report.records_seen,
        "timeline": report.timeline,
    }


def _cache_trace_round(round_id: str, payload: dict[int, dict], outcomes: dict[int, dict]) -> None:
    now = time.monotonic()
    while _TRACE_CACHE:
        oldest_id, oldest = next(iter(_TRACE_CACHE.items()))
        fresh = now - oldest["created_mono"] <= TRACE_CACHE_TTL_S
        if fresh and len(_TRACE_CACHE) < TRACE_CACHE_MAX_ROUNDS:
            break
        del _TRACE_CACHE[oldest_id]
    _TRACE_CACHE[round_id] = {
        "created_mono": now,
        "sources": payload,
        "outcomes": outcomes,
    }


async def _trace_source(
    src: SourceEndpoint,
    res: ProbeResult | ProbeError | BaseException,
    boot_before: str | None,
) -> tuple[ProbeOutcome, LinkReport]:
    outcome = _outcome_of(res)
    if outcome.trace_id is None:
        return outcome, analyze_link(outcome, None, boot_before=boot_before)
    trace_query, trace_error = await _fetch_trace(src, outcome.trace_id)
    return outcome, analyze_link(
        outcome, trace_query, trace_error=trace_error, boot_before=boot_before,
    )


@app.post("/api/diagnose")
async def diagnose(req: DiagnoseRequest) -> dict:
    timeout_s = req.timeout_ms / 1000.0
    sources = [SOURCES[1], SOURCES[2]]
    round_id = secrets.token_hex(8)

    # 探测前先取各源启动标识：探测期间源若重启，boot_id 变化，记录不采信
    boots = await asyncio.gather(*(_fetch_boot_id(s) for s in sources))

    # 两源并行探测；每个探测独立 socket（线程内阻塞式），互不串包。
    # return_exceptions=True：单源失败不拖垮整轮诊断，统一按四态判定处理。
    results = await asyncio.gather(
        *(asyncio.to_thread(probe_source, s.udp_host, s.udp_port, timeout_s)
          for s in sources),
        return_exceptions=True,
    )

    # 按每源各自的请求标识查询源端记录；轮与轮之间标识不同，不借用别轮记录
    traced = await asyncio.gather(
        *(_trace_source(src, res, boot) for src, res, boot in zip(sources, results, boots))
    )

    reports: list[dict] = []
    intervals: list[tuple[float, float] | None] = []
    links: dict[int, dict] = {}
    outcomes: dict[int, dict] = {}
    for src, res, (outcome, link_report) in zip(sources, results, traced):
        if isinstance(res, ProbeResult):
            reports.append(_source_report_ok(src, res))
            intervals.append((res.low_s * 1000.0, res.high_s * 1000.0))
        else:  # ProbeError / 意料外异常都按探测失败处理，绝不影响其他请求
            reports.append(_source_report_err(src, res))
            intervals.append(None)
        links[src.sid] = _link_payload(src, link_report, round_id, outcome)
        outcomes[src.sid] = _outcome_payload(outcome)

    decision = decide(intervals, req.tolerance_ms)
    _cache_trace_round(round_id, links, outcomes)
    return {
        "round_id": round_id,
        "tolerance_ms": req.tolerance_ms,
        "timeout_ms": req.timeout_ms,
        "sources": reports,
        "links": list(links.values()),
        "intersection_ms": list(decision.intersection_ms) if decision.intersection_ms else None,
        "verdict": decision.verdict.value,
        "verdict_text": VERDICT_TEXT[decision.verdict],
        "reason": decision.reason,
    }


@app.post("/api/trace")
async def trace_round(req: TraceQueryRequest) -> dict:
    """对指定诊断轮次、指定源重新查询源端收发记录。

    只认本进程记录的该轮请求标识，避免借用别轮或他源记录；回包/记录
    只用于解释链路，不改变该轮诊断判定。
    """
    entry = _TRACE_CACHE.get(req.round_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="该诊断轮次记录不存在或已过期")
    src = SOURCES.get(req.source_id)
    if src is None:
        raise HTTPException(status_code=404, detail=f"未知源 id={req.source_id}，仅支持 1/2")
    cached = entry["sources"].get(req.source_id)
    outcome_data = entry["outcomes"].get(req.source_id)
    if cached is None or outcome_data is None:
        raise HTTPException(status_code=404, detail="该轮未记录此源的追踪信息")
    trace_id = cached.get("trace_id")
    if not trace_id:
        raise HTTPException(status_code=409, detail="该轮探测未生成请求标识（意外异常），无法查询")

    trace_query, trace_error = await _fetch_trace(src, trace_id)
    outcome = ProbeOutcome(
        ok=outcome_data["ok"],
        error_kind=outcome_data["error_kind"],
        error_text=outcome_data["error_text"],
        trace_id=trace_id,
        t1_ntp=outcome_data["t1_ntp"],
        t1=outcome_data["t1"],
        t4=outcome_data["t4"],
    )
    report = analyze_link(
        outcome, trace_query, trace_error=trace_error,
        boot_before=cached.get("boot_before"),
    )
    return _link_payload(src, report, req.round_id, outcome)


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok"}
