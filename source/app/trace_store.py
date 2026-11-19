"""源端链路追踪记录：按请求标识登记收包 / 发包事实。

每个探测请求在 48 字节报文后携带 16 位十六进制请求标识（见
common.ntp_proto）。源端收到请求时登记一条“已收包”记录，真正调用
sendto 之前再补记“已发包”，供诊断端控制口按标识查询，区分：

- 源没见过这个请求（无记录）
- 源已收包但应答还没发出（记录有收包时间、无发包时间）
- 源已发包（两条时间都有）

记录只存在内存中，进程重启即清空；每条记录有存活时间（TTL），超过后
查询返回 expired。诊断端据此判断“证据是否足够”，不会把记录缺失直接
当成网络丢包。同一请求标识若重复出现，按 originate 时间戳分别登记，
诊断端只会采信与本轮 t1 完全一致的那条记录。
"""
from __future__ import annotations

import time
import uuid
from collections import OrderedDict

DEFAULT_TTL_S = 120.0
DEFAULT_RETAIN_EXPIRED_S = 300.0
DEFAULT_MAX_ENTRIES = 4096


class TraceStore:
    """有界、带 TTL 的追踪记录表（单事件循环内使用，无需加锁）。

    clock / wall_clock 可注入，便于单元测试时间流逝而不必真睡眠。
    """

    def __init__(
        self,
        source_id: str,
        *,
        ttl_s: float = DEFAULT_TTL_S,
        retain_expired_s: float = DEFAULT_RETAIN_EXPIRED_S,
        max_entries: int = DEFAULT_MAX_ENTRIES,
        clock=time.monotonic,
        wall_clock=time.time,
    ) -> None:
        self.source_id = source_id
        self.boot_id = uuid.uuid4().hex  # 每次进程启动（含重启）都不同
        self.ttl_s = max(0.0, ttl_s)
        self.retain_expired_s = max(0.0, retain_expired_s)
        self.max_entries = max(1, max_entries)
        self._clock = clock
        self._wall_clock = wall_clock
        self._entries: OrderedDict[str, dict] = OrderedDict()

    # ------------------------------------------------------------ 写入

    def record_received(
        self, trace_id: str, originate_ntp: int, receive_ntp: int,
    ) -> None:
        now = self._clock()
        self._prune(now)
        entry = self._entries.get(trace_id)
        if entry is None:
            entry = {
                "trace_id": trace_id,
                "created_mono": now,
                "created_at": self._wall_clock(),
                "records": OrderedDict(),
            }
            self._entries[trace_id] = entry
            self._enforce_capacity()
        entry["records"].setdefault(originate_ntp, {
            "originate": originate_ntp,
            "receive_ntp": receive_ntp,
            "transmit_ntp": None,
            "received_at": self._wall_clock(),
            "sent_at": None,
        })

    def record_sent(
        self, trace_id: str, originate_ntp: int, transmit_ntp: int,
    ) -> None:
        entry = self._entries.get(trace_id)
        if entry is None:  # 记录已被清理（TTL/容量）；不凭空补一条收包记录
            return
        record = entry["records"].get(originate_ntp)
        if record is None:
            return
        record["transmit_ntp"] = transmit_ntp
        record["sent_at"] = self._wall_clock()

    # ------------------------------------------------------------ 查询

    def query(self, trace_id: str) -> dict:
        now = self._clock()
        self._prune(now)
        entry = self._entries.get(trace_id)
        if entry is None:
            return {
                "source": self.source_id,
                "boot_id": self.boot_id,
                "trace_id": trace_id,
                "status": "not_found",
                "records": [],
            }
        age = now - entry["created_mono"]
        status = "ok" if age <= self.ttl_s else "expired"
        records = []
        for rec in entry["records"].values():
            item = dict(rec)
            item["state"] = "sent" if rec["sent_at"] is not None else "received"
            records.append(item)
        return {
            "source": self.source_id,
            "boot_id": self.boot_id,
            "trace_id": trace_id,
            "status": status,
            "age_ms": round(age * 1000.0, 3),
            "records": records,
        }

    # ------------------------------------------------------------ 内部

    def _prune(self, now: float) -> None:
        horizon = self.ttl_s + self.retain_expired_s
        while self._entries:
            trace_id, entry = next(iter(self._entries.items()))
            if now - entry["created_mono"] <= horizon:
                break
            del self._entries[trace_id]

    def _enforce_capacity(self) -> None:
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)
