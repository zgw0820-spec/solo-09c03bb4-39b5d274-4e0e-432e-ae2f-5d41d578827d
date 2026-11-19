"""对单台 NTP 时间源发起一次真实 UDP 探测，并做回包校验。

防串包设计：
- 每次探测新建独立 UDP socket（临时端口由内核分配，探测结束即关闭），
  并发诊断之间不存在共享 socket，内核按四元组投递，物理上不会串包；
- 回包必须原样回显我们发出的 originate 时间戳（t1，含随机小数部分），
  对不上即判为迟到/串包回包并拒绝；
- socket 超时之外的回包根本不会被读取，超时后到达的迟到包随 socket
  关闭被内核丢弃。
"""
from __future__ import annotations

import socket
import time
from dataclasses import dataclass

from common.ntp_proto import (
    MODE_SERVER,
    build_client_packet,
    ntp_to_unix,
    parse_packet,
    unix_to_ntp,
)


class ProbeError(Exception):
    """探测失败：超时 / 无效回包 / 迟到或串包回包。"""


@dataclass
class ProbeResult:
    """一次成功探测的四时间戳与派生量（秒存储，毫秒展示由上层负责）。"""

    t1: float  # 客户端发送时刻（本机钟）
    t2: float  # 服务器接收时刻（源钟）
    t3: float  # 服务器发送时刻（源钟）
    t4: float  # 客户端接收时刻（本机钟）
    offset_s: float   # θ = ((t2-t1)+(t3-t4))/2，源钟相对本机钟的钟差
    delay_s: float    # δ = (t4-t1)-(t3-t2)，往返网络时延
    low_s: float      # 真实钟差区间下界 θ-δ/2
    high_s: float     # 真实钟差区间上界 θ+δ/2


def probe_source(host: str, port: int, timeout_s: float) -> ProbeResult:
    """向 host:port 发一次 NTP 请求并校验回包。

    成功返回 ProbeResult；任何超时/无效/迟到回包都抛 ProbeError。
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.settimeout(timeout_s)
        t1 = time.time()
        t1_ntp = unix_to_ntp(t1)
        sock.sendto(build_client_packet(t1_ntp), (host, port))
        try:
            data, _peer = sock.recvfrom(2048)
        except socket.timeout as exc:
            raise ProbeError(f"回包超时（>{timeout_s * 1000:.0f} ms 未收到应答）") from exc
        except OSError as exc:
            raise ProbeError(f"网络错误：{exc}") from exc
        t4 = time.time()
    finally:
        sock.close()

    # ---- 回包有效性校验，任一不满足即拒绝 ----
    try:
        pkt = parse_packet(data)
    except ValueError as exc:
        raise ProbeError(f"无效回包：{exc}") from exc

    if pkt["mode"] != MODE_SERVER:
        raise ProbeError(f"无效回包：mode={pkt['mode']}，不是服务器应答")
    if not (1 <= pkt["stratum"] <= 15):
        raise ProbeError(f"无效回包：stratum={pkt['stratum']}（源未同步或不可信）")
    if pkt["originate"] != t1_ntp:
        raise ProbeError("迟到/串包回包：originate 时间戳与本轮请求不符，已拒绝")
    if pkt["receive"] == 0 or pkt["transmit"] == 0:
        raise ProbeError("无效回包：服务器时间戳为空")

    t2 = ntp_to_unix(pkt["receive"])
    t3 = ntp_to_unix(pkt["transmit"])

    if t3 < t2:
        raise ProbeError("无效回包：服务器发送时刻早于接收时刻")
    rtt = t4 - t1
    if rtt < 0:
        raise ProbeError("无效回包：本机时钟回拨，往返时延为负")
    if rtt > timeout_s:
        raise ProbeError(f"迟到回包：往返 {rtt * 1000:.1f} ms 超过超时 {timeout_s * 1000:.0f} ms")

    delay = rtt - (t3 - t2)
    if delay < 0:
        raise ProbeError("无效回包：推算网络时延为负")

    offset = ((t2 - t1) + (t3 - t4)) / 2.0
    half = delay / 2.0
    return ProbeResult(
        t1=t1, t2=t2, t3=t3, t4=t4,
        offset_s=offset, delay_s=delay,
        low_s=offset - half, high_s=offset + half,
    )
