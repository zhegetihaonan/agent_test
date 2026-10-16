from datetime import datetime

from .collectors import get_docker_logs, list_queues, load_daily_info
from .config import DOCKER_LOG_HOURS, QUEUE_PATTERN, THRESHOLD


def calculate_trends(history):
    if len(history) < 2:
        return {
            "history_days": len(history),
            "task_change_pct": None,
            "task_change_absolute": None,
            "task_change_note": "历史记录不足，无法判断任务量趋势",
            "detect_rate_change_points": None,
            "detect_rate_change_pct": None,
            "task_trend": "insufficient_data",
            "detect_rate_trend": "insufficient_data",
        }

    first = history[0]
    last = history[-1]

    task_change_absolute = last["tasks"] - first["tasks"]
    if first["tasks"] == 0:
        task_change = None
        if last["tasks"] == 0:
            task_trend = "no_tasks"
            task_change_note = "连续无任务；零基线不适用百分比变化"
        else:
            task_trend = "started_from_zero"
            task_change_note = "从零开始产生任务；零基线无法计算百分比变化"
    else:
        task_change = task_change_absolute / first["tasks"] * 100
        task_trend = (
            "rising" if task_change > 5
            else "falling" if task_change < -5
            else "stable"
        )
        task_change_note = None

    rate_change = last["detect_rate"] - first["detect_rate"]
    rate_change_pct = None
    if first["detect_rate"] != 0:
        rate_change_pct = rate_change / first["detect_rate"] * 100

    rate_trend = (
        "rising" if rate_change > 1
        else "falling" if rate_change < -1
        else "stable"
    )

    return {
        "history_days": len(history),
        "task_change_pct": round(task_change, 2) if task_change is not None else None,
        "task_change_absolute": task_change_absolute,
        "task_change_note": task_change_note,
        "detect_rate_change_points": round(rate_change, 4),
        "detect_rate_change_pct": round(rate_change_pct, 2) if rate_change_pct is not None else None,
        "task_trend": task_trend,
        "detect_rate_trend": rate_trend,
    }


def build_snapshot():
    queues = list_queues()
    task_queues = [
        q for q in queues
        if QUEUE_PATTERN.lower() in q["name"].lower()
    ]

    history = load_daily_info()
    logs = get_docker_logs()
    rule_findings = []

    for q in task_queues:
        q["over_threshold"] = q["messages"] > THRESHOLD
        if q["consumers"] == 0 and q["messages"] > 0:
            rule_findings.append({"queue": q["name"], "level": "ALERT", "reason": "队列有积压但消费者数为 0"})
        elif q["over_threshold"]:
            rule_findings.append({"queue": q["name"], "level": "REVIEW", "reason": f"积压超过参考阈值 {THRESHOLD:,}"})

    if len(history) < 2:
        data_status = "历史数据不足，无法判断趋势"
    else:
        data_status = f"已读取 {len(history)} 条历史记录"

    if not logs["available"]:
        rule_findings.append({
            "queue": None,
            "level": "ALERT",
            "reason": "RabbitMQ 容器日志不可用，无法判断日志异常",
        })
    elif logs["abnormal_line_count"]:
        rule_findings.append({"queue": None, "level": "REVIEW", "reason": f"近 {DOCKER_LOG_HOURS} 小时日志命中异常关键词 {logs['abnormal_line_count']} 行"})

    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "threshold": THRESHOLD,
        "queue_pattern": QUEUE_PATTERN,
        "task_queues": task_queues,
        "daily_history": history,
        "trends": calculate_trends(history),
        "docker_logs": logs,
        "data_status": data_status,
        "rule_findings": rule_findings,
    }
