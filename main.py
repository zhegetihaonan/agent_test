import json
import logging
import os
import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import requests
from openai import OpenAI


THRESHOLD = int(os.getenv("QUEUE_THRESHOLD", "500000"))
QUEUE_PATTERN = os.getenv("QUEUE_PATTERN", "task")
CONTAINER_NAME = os.getenv("RABBITMQ_CONTAINER", "rabbitmq")
DAILY_INFO_FILE = os.getenv("DAILY_INFO_FILE", "./data/daily_info.txt")
DOCKER_LOG_HOURS = int(os.getenv("DOCKER_LOG_HOURS", "24"))
DOCKER_LOG_MAX_CHARS = int(os.getenv("DOCKER_LOG_MAX_CHARS", "12000"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.6-luna")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "").strip()

WECOM_WEBHOOK = os.getenv("WECOM_WEBHOOK", "")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("rabbitmq-inspection-agent")


def run_command(cmd, timeout=30):
    logger.info("执行: %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"命令执行失败: {' '.join(cmd)}\n"
            f"stdout={result.stdout}\n"
            f"stderr={result.stderr}"
        )
    return result.stdout


def list_queues():
    output = run_command([
        "docker", "exec", CONTAINER_NAME,
        "rabbitmqctl", "list_queues", "name", "messages", "consumers"
    ])

    queues = []
    for raw in output.splitlines():
        line = raw.strip()
        if not line:
            continue

        parts = re.split(r"\s+", line)
        if len(parts) < 2:
            continue

        try:
            name = parts[0]
            messages = int(parts[1])
            consumers = int(parts[2]) if len(parts) >= 3 else None
        except ValueError:
            # 跳过 rabbitmqctl 可能出现的表头/异常行
            continue

        queues.append({
            "name": name,
            "messages": messages,
            "consumers": consumers,
        })

    return queues


def load_daily_info():
    """
    daily_info.txt 推荐格式：

    2026-10-07,1200000,96.2%
    2026-10-08,1100000,93.5%

    也支持：
    2026-10-07 1200000 96.2%
    2026-10-08 1100000 0.935

    返回最近 3 条有效记录。
    """
    path = Path(DAILY_INFO_FILE)
    if not path.exists():
        logger.warning("daily_info 文件不存在: %s", path)
        return []

    rows = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        parts = [p.strip() for p in re.split(r"[,，\t\s]+", line)]
        if len(parts) < 3:
            continue

        date_str, task_str, rate_str = parts[0], parts[1], parts[2]

        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            tasks = int(float(task_str.replace(",", "")))

            rate_str = rate_str.rstrip("%")
            rate = float(rate_str)
            if "%" not in parts[2] and rate <= 1:
                rate *= 100

            if not 0 <= rate <= 100:
                continue

            rows.append({
                "date": date_str,
                "tasks": tasks,
                "detect_rate": round(rate, 4),
            })
        except (ValueError, TypeError):
            continue

    rows.sort(key=lambda x: x["date"])
    return rows[-3:]


def get_docker_logs():
    """
    获取 RabbitMQ 容器最近 N 小时日志，并做关键词摘要。
    """
    since = f"{DOCKER_LOG_HOURS}h"
    output = run_command(
        ["docker", "logs", "--since", since, CONTAINER_NAME],
        timeout=60,
    )

    lines = output.splitlines()
    keywords = ("error", "exception", "timeout", "failed", "warning", "warn")
    abnormal = [
        line for line in lines
        if any(k in line.lower() for k in keywords)
    ]

    # 避免把巨量日志直接塞给模型
    abnormal_tail = abnormal[-300:]
    text = "\n".join(abnormal_tail)
    if len(text) > DOCKER_LOG_MAX_CHARS:
        text = text[-DOCKER_LOG_MAX_CHARS:]

    counts = {
        "error": sum("error" in x.lower() for x in abnormal),
        "exception": sum("exception" in x.lower() for x in abnormal),
        "timeout": sum("timeout" in x.lower() for x in abnormal),
        "failed": sum("failed" in x.lower() for x in abnormal),
        "warning": sum(
            ("warning" in x.lower()) or ("warn" in x.lower())
            for x in abnormal
        ),
    }

    return {
        "hours": DOCKER_LOG_HOURS,
        "total_log_lines": len(lines),
        "abnormal_line_count": len(abnormal),
        "keyword_counts": counts,
        "abnormal_samples": text,
    }


def calculate_trends(history):
    if len(history) < 2:
        return {
            "history_days": len(history),
            "task_change_pct": None,
            "detect_rate_change_points": None,
            "detect_rate_change_pct": None,
            "task_trend": "insufficient_data",
            "detect_rate_trend": "insufficient_data",
        }

    first = history[0]
    last = history[-1]

    task_change = None
    if first["tasks"] != 0:
        task_change = (last["tasks"] - first["tasks"]) / first["tasks"] * 100

    rate_change = last["detect_rate"] - first["detect_rate"]
    rate_change_pct = None
    if first["detect_rate"] != 0:
        rate_change_pct = rate_change / first["detect_rate"] * 100

    task_trend = "rising" if task_change > 5 else "falling" if task_change < -5 else "stable"
    rate_trend = (
        "rising" if rate_change > 1
        else "falling" if rate_change < -1
        else "stable"
    )

    return {
        "history_days": len(history),
        "task_change_pct": round(task_change, 2) if task_change is not None else None,
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

    for q in task_queues:
        q["over_threshold"] = q["messages"] > THRESHOLD

    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "threshold": THRESHOLD,
        "queue_pattern": QUEUE_PATTERN,
        "task_queues": task_queues,
        "daily_history": history,
        "trends": calculate_trends(history),
        "docker_logs": logs,
    }


SYSTEM_PROMPT = """
你是一个 RabbitMQ 爬虫系统运维巡检 Agent。

当前阶段是观察期，绝对禁止执行 purge，也没有 purge 工具。
你的任务是根据当前队列、过去三天任务量/检测率、任务趋势以及 RabbitMQ
容器最近日志，对每个 task 队列给出巡检建议。

请重点考虑：
1. 当前消息积压量不是唯一判断条件。
2. 如果任务量连续下降、检测率也连续下降，需要关注网址质量变化或检测链路异常。
3. 如果任务量连续上升、检测率稳定/上升，同时队列积压增加，可能只是正常流量增长，不应仅因超过阈值就建议清理。
4. 如果检测率明显下降，同时队列持续增长，优先提高告警等级。
5. 如果日志出现大量 timeout/error/failed/exception，优先考虑系统消费能力或依赖异常。
6. 当前只有最多三天历史数据，因此不要把趋势说成确定因果关系。
7. 数据不足时明确说“数据不足”，不要编造。
8. 不要仅仅因为超过 50 万就机械地给出 PURGE。
9. 这是生产系统巡检，宁可 ALERT，也不要在证据不足时给出过度确定的结论。

对每个 task 队列返回：
- queue
- decision: PURGE_RECOMMEND / KEEP / ALERT
- confidence: 0~1
- reason
- evidence: 最多 5 条关键证据
- next_action

最终还要给出 overall_summary。

只输出 JSON，不要 Markdown。
"""


def call_llm(snapshot):
    if not OPENAI_API_KEY:
        raise RuntimeError("未设置 OPENAI_API_KEY")

    client_kwargs = {"api_key": OPENAI_API_KEY}
    if OPENAI_BASE_URL:
        client_kwargs["base_url"] = OPENAI_BASE_URL

    client = OpenAI(**client_kwargs)

    response = client.responses.create(
        model=OPENAI_MODEL,
        instructions=SYSTEM_PROMPT,
        input=json.dumps(snapshot, ensure_ascii=False),
    )

    text = response.output_text.strip()

    # 兼容模型偶尔返回 ```json ... ```
    text = re.sub(r"^```json\s*", "", text)
    text = re.sub(r"^```\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"模型没有返回合法 JSON:\n{text}"
        ) from exc


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
            "### 过去三天趋势",
            f"- 任务量趋势：{trends['task_trend']}"
            + (
                f"（{trends['task_change_pct']}%）"
                if trends["task_change_pct"] is not None else ""
            ),
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

    lines.append("### 队列情况")

    result_items = decision.get("queues", [])
    by_name = {x.get("queue"): x for x in result_items}

    for q in snapshot["task_queues"]:
        name = q["name"]
        item = by_name.get(name, {})
        decision_name = item.get("decision", "ALERT")
        confidence = item.get("confidence", "-")
        lines.append(
            f"- **{name}**：{q['messages']:,} 条，"
            f"建议 `{decision_name}`，置信度 `{confidence}`"
        )
        if item.get("reason"):
            lines.append(f"  - 原因：{item['reason']}")
        if item.get("next_action"):
            lines.append(f"  - 建议：{item['next_action']}")

    summary = decision.get("overall_summary")
    if summary:
        lines.extend(["", "### 总结", summary])

    lines.extend([
        "",
        "> 当前为观察模式：Agent 只提供建议，不执行 purge_queue。",
    ])

    return "\n".join(lines)


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

    response = requests.post(
        WECOM_WEBHOOK,
        json=payload,
        timeout=15,
    )
    response.raise_for_status()

    result = response.json()
    if result.get("errcode") != 0:
        raise RuntimeError(f"企微发送失败: {result}")


def main():
    logger.info("开始 RabbitMQ Agent 巡检")

    snapshot = build_snapshot()

    logger.info(
        "发现 %d 个 task 队列，历史数据 %d 天",
        len(snapshot["task_queues"]),
        len(snapshot["daily_history"]),
    )

    decision = call_llm(snapshot)

    # 本地保存一份原始巡检结果，方便回溯 Agent 为什么这么判断
    Path("data").mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    Path(f"data/report_{ts}.json").write_text(
        json.dumps(
            {
                "snapshot": snapshot,
                "decision": decision,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    report = build_wecom_markdown(snapshot, decision)
    send_wecom(report)

    print(report)


if __name__ == "__main__":
    main()
