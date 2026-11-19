"""NTP 协议公共工具：时间戳转换与 48 字节报文编解码。

诊断端（client）与时间源端（server）共用，保证两端对报文格式
的理解完全一致。仅实现本系统需要的 NTPv4 子集。
"""
from __future__ import annotations

import struct

# 1900-01-01 到 1970-01-01 的秒数（NTP 纪元与 Unix 纪元之差）
NTP_EPOCH_DELTA = 2_208_988_800

NTP_PACKET_SIZE = 48

MODE_CLIENT = 3
MODE_SERVER = 4
VERSION = 4

# 头 4 字节 + 根延迟/根离散各 4 + 参考 ID 4 + 4 个 64 位时间戳 = 48 字节
# B   B   b   b   I           I              4s      Q Q Q Q
# li  stratum poll precision  root_delay     root_disp ref_id  ref/orig/recv/xmit
_PACKET = struct.Struct("!BBbbII4sQQQQ")


def unix_to_ntp(ts: float) -> int:
    """Unix 时间（float，秒）-> 64 位 NTP 时间戳（整数）。"""
    ntp = ts + NTP_EPOCH_DELTA
    seconds = int(ntp)
    fraction = int(round((ntp - seconds) * (1 << 32)))
    if fraction >= (1 << 32):  # 四舍五入进位
        seconds += 1
        fraction -= 1 << 32
    return ((seconds & 0xFFFFFFFF) << 32) | fraction


def ntp_to_unix(ntp: int) -> float:
    """64 位 NTP 时间戳 -> Unix 时间（float，秒）。"""
    seconds = (ntp >> 32) & 0xFFFFFFFF
    fraction = ntp & 0xFFFFFFFF
    return seconds - NTP_EPOCH_DELTA + fraction / (1 << 32)


def build_client_packet(transmit_ntp: int) -> bytes:
    """构造客户端请求：LI=0, VN=4, Mode=3，仅填发送时间戳。"""
    li_vn_mode = (0 << 6) | (VERSION << 3) | MODE_CLIENT
    return _PACKET.pack(li_vn_mode, 0, 0, 0, 0, 0, b"\x00" * 4, 0, 0, 0, transmit_ntp)


def build_server_packet(
    originate_ntp: int,
    receive_ntp: int,
    transmit_ntp: int,
    stratum: int = 2,
    ref_id: bytes = b"LCL\x00",
) -> bytes:
    """构造服务器应答：Mode=4，回显 originate，填接收/发送时间戳。"""
    li_vn_mode = (0 << 6) | (VERSION << 3) | MODE_SERVER
    return _PACKET.pack(
        li_vn_mode, stratum, 6, -20, 0, 0, ref_id,
        transmit_ntp, originate_ntp, receive_ntp, transmit_ntp,
    )


def parse_packet(data: bytes) -> dict:
    """解析 NTP 报文，长度不足直接抛 ValueError。"""
    if len(data) < NTP_PACKET_SIZE:
        raise ValueError(f"packet too short: {len(data)} bytes")
    (li_vn_mode, stratum, poll, precision, root_delay, root_disp,
     ref_id, ref_ts, originate, receive, transmit) = _PACKET.unpack(data[:NTP_PACKET_SIZE])
    return {
        "leap": (li_vn_mode >> 6) & 0x3,
        "version": (li_vn_mode >> 3) & 0x7,
        "mode": li_vn_mode & 0x7,
        "stratum": stratum,
        "poll": poll,
        "precision": precision,
        "root_delay": root_delay,
        "root_dispersion": root_disp,
        "ref_id": ref_id,
        "reference": ref_ts,
        "originate": originate,
        "receive": receive,
        "transmit": transmit,
    }
