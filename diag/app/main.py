"""诊断服务：操作网页 + API。

- GET  /                       操作网页
- GET  /api/sources            两台时间源的当前配置（实时取自源端控制口）
- POST /api/sources/{id}/config 调整指定源的钟差 / 回包延迟
- POST /api/diagnose           发起一轮真实 UDP 探测并给出四态判定

并发说明：每次 /api/diagnose 为每个源各开一个独立 UDP socket 探测
（见 probe.py），请求之间无任何共享状态，可安全并发。
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

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
_STATIC = Path(__file__).resolve().parent / "static"

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
    return {"id": src.sid, "name": src.name, "ok": False, "error": str(err)}


@app.post("/api/diagnose")
async def diagnose(req: DiagnoseRequest) -> dict:
    timeout_s = req.timeout_ms / 1000.0
    sources = [SOURCES[1], SOURCES[2]]

    # 两源并行探测；每个探测独立 socket（线程内阻塞式），互不串包。
    # return_exceptions=True：单源失败不拖垮整轮诊断，统一按四态判定处理。
    results = await asyncio.gather(
        *(asyncio.to_thread(probe_source, s.udp_host, s.udp_port, timeout_s) for s in sources),
        return_exceptions=True,
    )

    reports: list[dict] = []
    intervals: list[tuple[float, float] | None] = []
    for src, res in zip(sources, results):
        if isinstance(res, ProbeResult):
            reports.append(_source_report_ok(src, res))
            intervals.append((res.low_s * 1000.0, res.high_s * 1000.0))
        elif isinstance(res, ProbeError):
            reports.append(_source_report_err(src, res))
            intervals.append(None)
        else:  # 意料外异常也按探测失败处理，绝不让一次诊断影响其他请求
            reports.append(_source_report_err(src, res))
            intervals.append(None)

    decision = decide(intervals, req.tolerance_ms)
    return {
        "tolerance_ms": req.tolerance_ms,
        "timeout_ms": req.timeout_ms,
        "sources": reports,
        "intersection_ms": list(decision.intersection_ms) if decision.intersection_ms else None,
        "verdict": decision.verdict.value,
        "verdict_text": VERDICT_TEXT[decision.verdict],
        "reason": decision.reason,
    }


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok"}
