import re
import time
from datetime import datetime

import requests

from .config import (
    OPENAI_API_KEY,
    THRESHOLD,
    WECOM_MAX_ATTEMPTS,
    WECOM_WEBHOOK,
    logger,
)


def build_wecom_markdown(snapshot, decision):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "## RabbitMQ Agent 巡检报告",
        f"> 检查时间：{now}",
        f"> 阈值参考：{THRESHOLD:,} 条",
        "",
    ]

    trends = snapshot["trends"]
    if trends["history_days"]:
        lines.extend([
            "### 过去七天趋势",
            f"- 任务量趋势：{format_task_trend(trends)}",
            f"- 检测率趋势：{trends['detect_rate_trend']}"
            + (
                f"（{trends['detect_rate_change_points']} 个百分点）"
                if trends["detect_rate_change_points"] is not None else ""
            ),
            "",
        ])
    else:
        lines.extend([
            "### 历史数据",
            "- 未读取到有效的 daily_info.txt 数据",
            "",
        ])

    lines.append("### 数据采集")
    lines.append(f"- {snapshot.get('data_status', '未知')}")
    logs = snapshot["docker_logs"]
    if logs["available"]:
        lines.append(
            f"- RabbitMQ 日志：{logs['total_log_lines']} 行，"
            f"关键词命中 {logs['abnormal_line_count']} 行"
        )
    else:
        lines.append(f"- RabbitMQ 日志不可用：{logs.get('error') or '未提供错误详情'}")
    findings = snapshot.get("rule_findings", [])
    if findings:
        lines.extend(["", "### 规则提醒"])
        for finding in findings:
            scope = f"[{finding['queue']}] " if finding.get("queue") else ""
            lines.append(
                f"- {finding['level']}：{scope}{finding['reason']}"
            )
    lines.extend(["", "### 队列情况"])

    result_items = decision.get("queues", [])
    by_name = {x.get("queue"): x for x in result_items}

    for q in snapshot["task_queues"]:
        name = q["name"]
        item = by_name.get(name, {})
        decision_name = item.get("decision", "ALERT")
        confidence = item.get("confidence", "-")
        lines.append(
            f"- **{name}**：{q['messages']:,} 条，"
            f"消费者 {q['consumers'] if q['consumers'] is not None else '未知'}，"
            f"建议 `{decision_name}`，置信度 `{confidence}`"
        )
        if item.get("reason"):
            lines.append(f"  - 原因：{item['reason']}")
        if item.get("next_action"):
            lines.append(f"  - 建议：{item['next_action']}")
        if item.get("evidence"):
            lines.append("  - 证据：" + "；".join(str(x) for x in item["evidence"][:5]))

    summary = decision.get("overall_summary")
    if summary:
        lines.extend(["", "### 总结", summary])

    lines.extend([
        "",
        "> 当前为观察模式：Agent 只提供建议，不执行 purge_queue。",
    ])

    return "\n".join(lines)


def format_task_trend(trends):
    labels = {
        "rising": "上升",
        "falling": "下降",
        "stable": "稳定",
        "no_tasks": "持续无任务",
        "started_from_zero": "从零开始产生任务",
        "insufficient_data": "数据不足",
    }
    label = labels.get(trends["task_trend"], trends["task_trend"])
    change_pct = trends.get("task_change_pct")
    if change_pct is not None:
        label += f"（{change_pct}%）"
    absolute = trends.get("task_change_absolute")
    if absolute is not None:
        label += f"，绝对变化 {absolute:+,} 条"
    note = trends.get("task_change_note")
    if note:
        label += f"；{note}"
    return label


def send_wecom(content):
    if not WECOM_WEBHOOK:
        logger.warning("未设置 WECOM_WEBHOOK，跳过企微发送")
        return

    payload = {
        "msgtype": "markdown",
        "markdown": {
            "content": content,
        },
    }

    for attempt in range(1, WECOM_MAX_ATTEMPTS + 1):
        try:
            response = requests.post(
                WECOM_WEBHOOK,
                json=payload,
                timeout=15,
            )
            response.raise_for_status()

            result = response.json()
            if result.get("errcode") != 0:
                raise RuntimeError(f"企微发送失败: {result}")
            return
        except (requests.RequestException, RuntimeError) as exc:
            if attempt == WECOM_MAX_ATTEMPTS:
                raise
            delay = 2 ** (attempt - 1)
            logger.warning(
                "企微发送失败（%d/%d），%d 秒后重试：%s",
                attempt,
                WECOM_MAX_ATTEMPTS,
                delay,
                type(exc).__name__,
            )
            time.sleep(delay)


def build_failure_report(stage, error):
    detail = str(error).replace(WECOM_WEBHOOK, "[webhook已隐藏]")
    if OPENAI_API_KEY:
        detail = detail.replace(OPENAI_API_KEY, "[API Key已隐藏]")
    detail = re.sub(r"https?://\S+", "[链接已隐藏]", detail)[:500]
    return "\n".join([
        "## RabbitMQ Agent 巡检失败",
        f"> 时间：{datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"> 失败阶段：{stage}",
        f"> 错误类型：{type(error).__name__}",
        f"> 错误摘要：{detail or '无详细错误信息'}",
        "",
        "本次巡检未能完整完成，请检查运行日志和依赖服务。",
    ])
