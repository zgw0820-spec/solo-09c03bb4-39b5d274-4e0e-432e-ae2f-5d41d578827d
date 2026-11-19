"""本地 NTP 时间源：UDP NTP 服务 + HTTP 控制口。

- UDP 端口（默认 123）：应答标准 NTP 客户端请求。应答中的 t2/t3 均按
  “真实时间 + 配置钟差” 填写；“回包延迟”按网络时延模拟——t3 在收到
  请求时即盖章，随后让应答包滞留 delay 再发出，使延迟体现在
  δ=(t4-t1)-(t3-t2) 中，从而拉宽诊断端的不确定区间（若先睡眠再盖
  t3，延迟会被当作服务器处理时间从 δ 中扣除，失去模拟意义）。
- HTTP 端口（默认 9000）：GET/POST /config 实时查看与调整
  offset_ms（钟差）和 reply_delay_ms（回包延迟）；GET /trace/{请求标识}
  按本轮请求标识返回收包 / 发包记录，供诊断端区分链路各环节。

同一事件循环内并发处理多个请求，每个应答任务独立睡眠、独立取时，
并发诊断不会互相干扰。
"""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from common.ntp_proto import (
    MODE_CLIENT,
    TRACE_ID_RE,
    build_server_packet,
    extract_trace_id,
    parse_packet,
    unix_to_ntp,
)
from .trace_store import TraceStore

SOURCE_ID = os.getenv("SOURCE_ID", "ntp-source")
UDP_HOST = os.getenv("UDP_HOST", "0.0.0.0")
UDP_PORT = int(os.getenv("UDP_PORT", "123"))
HTTP_PORT = int(os.getenv("HTTP_PORT", "9000"))
TRACE_TTL_S = float(os.getenv("TRACE_TTL_S", "120"))
TRACE_RETAIN_EXPIRED_S = float(os.getenv("TRACE_RETAIN_EXPIRED_S", "300"))
TRACE_MAX_ENTRIES = int(os.getenv("TRACE_MAX_ENTRIES", "4096"))

TRACES = TraceStore(
    SOURCE_ID, ttl_s=TRACE_TTL_S, retain_expired_s=TRACE_RETAIN_EXPIRED_S,
    max_entries=TRACE_MAX_ENTRIES,
)


class SourceState:
    """运行时可调参数。float 赋值在 CPython 下原子，无需加锁。"""

    offset_ms: float = float(os.getenv("INITIAL_OFFSET_MS", "0"))
    reply_delay_ms: float = float(os.getenv("INITIAL_DELAY_MS", "20"))


class NtpDatagram(asyncio.DatagramProtocol):
    """无状态 NTP 应答器：每个数据报独立处理。"""

    def __init__(self) -> None:
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr) -> None:  # noqa: ANN001
        try:
            pkt = parse_packet(data)
        except ValueError:
            return  # 非法请求直接丢弃
        if pkt["mode"] != MODE_CLIENT:
            return  # 只应答客户端请求
        # 在收到请求的同一时刻快照配置，保证本次应答的 t2/t3 用同一钟差
        offset_s = SourceState.offset_ms / 1000.0
        delay_s = SourceState.reply_delay_ms / 1000.0
        t2 = time.time() + offset_s
        t2_ntp = unix_to_ntp(t2)
        trace_id = extract_trace_id(data)
        if trace_id is not None:
            # 先登记“已收包”，随后的发包记录由应答任务补充
            TRACES.record_received(trace_id, pkt["transmit"], t2_ntp)
        # NTP 约定：应答的 originate 字段回显客户端请求里的 transmit 时间戳
        asyncio.get_running_loop().create_task(
            self._respond(pkt["transmit"], t2, offset_s, delay_s, addr, trace_id)
        )

    async def _respond(self, originate_ntp: int, t2: float, offset_s: float,
                       delay_s: float, addr, trace_id: str | None = None) -> None:  # noqa: ANN001
        # 先盖 t3（处理时间≈0），再让回包滞留 delay_s —— 模拟回程网络时延
        t3 = time.time() + offset_s
        if delay_s > 0:
            await asyncio.sleep(delay_s)
        t3_ntp = unix_to_ntp(t3)
        reply = build_server_packet(originate_ntp, unix_to_ntp(t2), t3_ntp)
        if self.transport is not None:
            # 发包成功后才补记“已发包”；transport.close() 之后 sendto 不抛错
            # 但也不会真正发出，因此以 is_closing() 判定
            if not self.transport.is_closing():
                self.transport.sendto(reply, addr)
                if trace_id is not None:
                    TRACES.record_sent(trace_id, originate_ntp, t3_ntp)


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        NtpDatagram, local_addr=(UDP_HOST, UDP_PORT)
    )
    app.state.udp_transport = transport
    yield
    transport.close()


app = FastAPI(title=f"NTP 时间源 {SOURCE_ID}", lifespan=lifespan)


class ConfigUpdate(BaseModel):
    offset_ms: float | None = Field(default=None, ge=-3_600_000, le=3_600_000)
    reply_delay_ms: float | None = Field(default=None, ge=0, le=60_000)


@app.get("/health")
async def health() -> dict:
    # boot_id 每次进程启动都不同：诊断端据此识别源是否在探测期间重启
    return {"status": "ok", "source": SOURCE_ID, "boot_id": TRACES.boot_id}


@app.get("/config")
async def get_config() -> dict:
    return {
        "source": SOURCE_ID,
        "offset_ms": SourceState.offset_ms,
        "reply_delay_ms": SourceState.reply_delay_ms,
    }


@app.post("/config")
async def set_config(update: ConfigUpdate) -> dict:
    if update.offset_ms is not None:
        SourceState.offset_ms = update.offset_ms
    if update.reply_delay_ms is not None:
        SourceState.reply_delay_ms = update.reply_delay_ms
    return await get_config()


@app.get("/trace/{trace_id}")
async def get_trace(trace_id: str) -> dict:
    """按请求标识查询本源的收包 / 发包记录。

    status: ok（记录在有效期内）/ not_found（没见过该请求，或已被清理）
    / expired（见过但记录已过期，不能作为本轮证据）。boot_id 供诊断端
    识别源在探测期间是否重启过。
    """
    if not TRACE_ID_RE.fullmatch(trace_id):
        raise HTTPException(status_code=400, detail="请求标识格式非法（需 16 位小写十六进制）")
    return TRACES.query(trace_id)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT)
