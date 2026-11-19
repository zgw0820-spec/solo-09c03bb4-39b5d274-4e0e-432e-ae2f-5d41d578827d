"""校时判定逻辑（纯函数，不依赖网络，便于单元测试）。

输入两源各自的真实钟差不确定区间 [low, high]（毫秒，源钟 - 诊断机钟），
输出四态判定：

- 任一源探测失败            -> UNKNOWN  无法判断
- 两源区间不相交            -> UNKNOWN  无法判断（两源互相矛盾）
- 交集 ⊆ [-tol, +tol]       -> OK       无需校时
- 交集 ∩ [-tol, +tol] = ∅   -> NEED     需要校时
- 交集跨越容差边界          -> PENDING  待核（人工复核）
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Verdict(str, Enum):
    OK = "no_calibration_needed"   # 无需校时
    NEED = "calibration_needed"    # 需要校时
    UNKNOWN = "undetermined"       # 无法判断
    PENDING = "pending_review"     # 待核


VERDICT_TEXT = {
    Verdict.OK: "无需校时",
    Verdict.NEED: "需要校时",
    Verdict.UNKNOWN: "无法判断",
    Verdict.PENDING: "待核",
}

_EPS = 1e-9  # 浮点边界容差


@dataclass
class Decision:
    verdict: Verdict
    reason: str
    intersection_ms: tuple[float, float] | None  # 两源区间交集；无交集或无法判断时为 None


def decide(
    intervals_ms: list[tuple[float, float] | None],
    tolerance_ms: float,
) -> Decision:
    """综合两源区间与容许偏差给出判定。

    intervals_ms: 每源一项，成功为 (low, high)，失败为 None。
    """
    failed = [i + 1 for i, iv in enumerate(intervals_ms) if iv is None]
    if failed:
        return Decision(
            Verdict.UNKNOWN,
            f"源 {', '.join(map(str, failed))} 探测失败（超时或回包异常），无法判断",
            None,
        )

    lows = [iv[0] for iv in intervals_ms if iv is not None]
    highs = [iv[1] for iv in intervals_ms if iv is not None]
    lo, hi = max(lows), min(highs)

    if lo > hi + _EPS:
        return Decision(
            Verdict.UNKNOWN,
            f"两源钟差区间不相交（[{lows[0]:.3f}, {highs[0]:.3f}] 与 "
            f"[{lows[1]:.3f}, {highs[1]:.3f}] ms），两源互相矛盾，无法判断",
            None,
        )
    if lo > hi:  # 浮点毛刺（差距 < _EPS）：把交集收成一点
        lo = hi

    tol = abs(tolerance_ms)
    if lo >= -tol - _EPS and hi <= tol + _EPS:
        return Decision(
            Verdict.OK,
            f"两源交集 [{lo:.3f}, {hi:.3f}] ms 完全落在容许偏差 ±{tol:.3f} ms 内，无需校时",
            (lo, hi),
        )
    if lo > tol + _EPS or hi < -tol - _EPS:
        return Decision(
            Verdict.NEED,
            f"两源交集 [{lo:.3f}, {hi:.3f}] ms 完全落在容许偏差 ±{tol:.3f} ms 之外，需要校时",
            (lo, hi),
        )
    return Decision(
        Verdict.PENDING,
        f"两源交集 [{lo:.3f}, {hi:.3f}] ms 跨越容许偏差边界 ±{tol:.3f} ms，"
        "网络时延不确定性不足以定论，保持待核，请人工复核或重测",
        (lo, hi),
    )
