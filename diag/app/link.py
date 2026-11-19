"""链路追踪状态判定（纯函数，不依赖网络，便于单元测试）。

输入诊断端本轮探测结果、源端控制口按**本轮请求标识**查回的记录，以及
探测前后源的启动标识，输出一条可解释的链路状态。

设计原则：
- 只有记录可采信时（控制口可达、记录未过期、源未在探测期间重启、
  记录 originate 与本轮 t1 一致），才根据“收包/发包”事实给出具体环节；
- 控制口不可达、源重启、记录过期、记录缺失或与本轮 t1 对不上时，
  一律只报“证据不足”，不据此断定网络丢包；
- 同一标识下若只有别轮 originate 的记录，绝不借用。
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class LinkState:
    code: str
    label: str
    detail: str
    conclusive: bool  # 记录是否可采信为“本轮”证据


STATE_LABELS = {
    "source_no_request": "源未见请求",
    "source_received_not_sent": "源已收包但尚未发出",
    "source_sent_diag_not_received": "源已发包但诊断机未按时收到",
    "source_sent_diag_late": "源已发包，诊断机迟到收到并拒收",
    "source_sent_diag_invalid_reply": "源已发包，诊断机收到但回包无效",
    "source_sent_diag_received": "源已发包且诊断机按时收到",
    "insufficient_evidence": "证据不足",
}


def _insufficient(detail: str) -> LinkState:
    return LinkState("insufficient_evidence", STATE_LABELS["insufficient_evidence"],
                     detail, conclusive=False)


def _state(code: str, detail: str) -> LinkState:
    return LinkState(code, STATE_LABELS[code], detail, conclusive=True)


@dataclass
class ProbeOutcome:
    """探测结果的精简视图，隔离对 ProbeResult/ProbeError 的依赖。"""

    ok: bool
    error_kind: str | None = None      # timeout/network/invalid_reply/late_reply
    error_text: str | None = None
    trace_id: str | None = None
    t1_ntp: int | None = None
    t1: float | None = None
    t4: float | None = None
    error: Exception | None = None


@dataclass
class LinkReport:
    state: LinkState
    trace_id: str | None
    boot_before: str | None
    boot_after: str | None
    record_status: str | None = None
    record: dict | None = None
    records_seen: list[dict] = field(default_factory=list)
    control_reachable: bool = True
    timeline: dict = field(default_factory=dict)


def analyze_link(
    outcome: ProbeOutcome,
    trace_query: dict | None,
    *,
    trace_error: str | None = None,
    boot_before: str | None,
) -> LinkReport:
    """综合探测结果与源端追踪记录，给出链路状态。

    trace_query 为源端 /trace/{id} 的返回（None 表示查询失败）；
    boot_before 为探测前源端 /health 返回的启动标识（None 表示未取到）。
    """
    timeline = {
        "request_sent_at": outcome.t1,
        "source_received_at": None,
        "source_sent_at": None,
        "diag_received_at": outcome.t4,
        "diag_received_valid": bool(outcome.ok),
    }
    report = LinkReport(
        state=_insufficient("尚未取得追踪记录"),
        trace_id=outcome.trace_id,
        boot_before=boot_before,
        boot_after=(trace_query or {}).get("boot_id"),
        record_status=(trace_query or {}).get("status"),
        timeline=timeline,
    )

    if outcome.error is not None and outcome.trace_id is None:
        report.state = _insufficient(
            f"探测出现意外异常，未能生成可追踪的请求标识：{outcome.error}"
        )
        return report
    if trace_error is not None or trace_query is None:
        report.control_reachable = False
        report.state = _insufficient(
            f"源端控制口不可达，无法核对本轮收发记录：{trace_error or '查询失败'}"
            "；不能据此断定网络丢包"
        )
        return report

    status = trace_query.get("status")
    boot_after = trace_query.get("boot_id")
    records = trace_query.get("records") or []
    report.records_seen = records

    if status == "expired":
        report.state = _insufficient(
            "源端记录已过期（TTL），只能确认源曾见过该标识，不能作为本轮证据"
        )
        return report
    if boot_before is None:
        report.state = _insufficient(
            "探测前未能读取源端启动标识，无法排除源在探测期间重启；记录不采信"
        )
        return report
    if boot_after != boot_before:
        report.state = _insufficient(
            "源在探测期间重启（启动标识变化），重启前后的记录不代表同一轮请求；"
            "不能据此断定网络丢包"
        )
        return report

    if status == "not_found":
        if outcome.ok:
            report.state = _insufficient(
                "诊断机已收到回包，但源端查无本轮请求记录，证据自相矛盾"
            )
        else:
            report.state = _state(
                "source_no_request",
                "源端查无本轮请求记录：请求未到达源端（去程丢包、端口/地址错误或"
                "源端未启用追踪），不涉及源端回程",
            )
        return report

    # 只采信 originate 与本轮 t1 完全一致的记录，防止借用别轮记录
    matching = [r for r in records if outcome.t1_ntp is not None
                and r.get("originate") == outcome.t1_ntp]
    if not matching:
        report.state = _insufficient(
            "源端无与本轮 t1 对应的记录（现有记录属于其他轮次），不借用别轮记录"
        )
        return report

    record = matching[-1]
    report.record = record
    timeline["source_received_at"] = record.get("received_at")
    timeline["source_sent_at"] = record.get("sent_at")

    sent = record.get("sent_at") is not None
    if not sent:
        if outcome.ok:
            report.state = _insufficient(
                "诊断机已收到回包，但源端记录仍显示未发包，证据自相矛盾"
            )
        else:
            report.state = _state(
                "source_received_not_sent",
                "查询时源端已收到本轮请求，但应答尚未发出（可能仍在回包延迟中）；"
                "此时不能断定去程或回程丢包",
            )
        return report

    if outcome.ok:
        report.state = _state(
            "source_sent_diag_received",
            "源端已发包，诊断机在超时内收到并通过校验",
        )
        return report

    if outcome.error_kind == "late_reply":
        report.state = _state(
            "source_sent_diag_late",
            "源端已发包，诊断机虽收到但超出超时或 originate 不符，已按迟到/串包拒收",
        )
    elif outcome.error_kind == "invalid_reply":
        report.state = _state(
            "source_sent_diag_invalid_reply",
            "源端已发包，诊断机收到回包但校验未通过（格式/模式/时间戳无效）",
        )
    elif outcome.error_kind == "timeout":
        report.state = _state(
            "source_sent_diag_not_received",
            "源端已发包，但诊断机在超时内未收到：问题在回程链路或诊断机收包环节",
        )
    else:
        report.state = _insufficient(
            f"探测失败（{outcome.error_text or '未知原因'}），且无法从记录进一步归因"
        )
    return report
