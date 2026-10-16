import json
import logging
import os
import re
import subprocess
import time
from datetime import datetime
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
WECOM_MAX_ATTEMPTS = 3

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
        check=False,
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

    2026-10-07,720000,12.6%
    2026-10-08,810000,13.1%

    也支持：
    2026-10-07 720000 12.6%
    2026-10-08 810000 0.131

    返回最近 7 条有效记录。
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
    return rows[-7:]


def get_docker_logs():
    """
    获取 RabbitMQ 容器最近 N 小时日志，合并 stdout/stderr 并做关键词摘要。
    """
    since = f"{DOCKER_LOG_HOURS}h"
    cmd = ["docker", "logs", "--since", since, CONTAINER_NAME]
    logger.info("执行: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=60,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        logger.warning("Docker 日志采集失败: %s", type(exc).__name__)
        return unavailable_docker_logs(f"{type(exc).__name__}: {exc}")

    output = result.stdout or ""
    if result.returncode != 0:
        detail = output.strip()[-500:] or "命令未返回错误详情"
        logger.warning("Docker 日志采集失败，退出码 %s", result.returncode)
        return unavailable_docker_logs(
            f"docker logs 退出码 {result.returncode}: {detail}",
            exit_code=result.returncode,
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
        "available": True,
        "exit_code": result.returncode,
        "error": None,
        "total_log_lines": len(lines),
        "abnormal_line_count": len(abnormal),
        "keyword_counts": counts,
        "abnormal_samples": text,
    }


def unavailable_docker_logs(error, exit_code=None):
    return {
        "hours": DOCKER_LOG_HOURS,
        "available": False,
        "exit_code": exit_code,
        "error": str(error)[:500],
        "total_log_lines": None,
        "abnormal_line_count": None,
        "keyword_counts": {},
        "abnormal_samples": "",
    }


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


SYSTEM_PROMPT = """
你是一个 RabbitMQ 爬虫系统运维巡检 Agent。

当前阶段是观察期，绝对禁止执行 purge，也没有 purge 工具。
你的任务是根据当前队列、过去七天任务量/检测率、任务趋势以及 RabbitMQ
容器最近日志，对每个 task 队列给出巡检建议。

请重点考虑：
1. 当前消息积压量不是唯一判断条件。
2. 如果任务量连续下降、检测率也连续下降，需要关注网址质量变化或检测链路异常。
3. 如果任务量连续上升、检测率稳定/上升，同时队列积压增加，可能只是正常流量增长，不应仅因超过阈值就建议清理。
4. 如果检测率明显下降，同时队列持续增长，优先提高告警等级。
5. 如果日志出现大量 timeout/error/failed/exception，优先考虑系统消费能力或依赖异常。
6. task_trend 为 no_tasks 时表示首尾均无任务；为 started_from_zero 时表示任务从零开始出现。零基线时不要编造百分比变化，应结合 task_change_absolute 描述。
7. docker_logs.available 为 false 时，日志证据不可用，不得当作“没有异常”；应明确提醒关注日志采集失败。
8. 代码提供的 rule_findings 是明确规则命中，必须纳入判断；规则命中不等于自动清队列。
9. 当前最多只有七条历史数据，因此不要把趋势说成确定因果关系。
10. 数据不足时明确说“数据不足”，不要编造。
11. 不要仅仅因为超过阈值就机械地给出 PURGE_RECOMMEND；当前始终是观察模式。
12. 这是生产系统巡检，宁可 ALERT，也不要在证据不足时给出过度确定的结论。

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

    client = OpenAI(**client_kwargs, timeout=45.0, max_retries=2)

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
        decision = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"模型没有返回合法 JSON:\n{text}"
        ) from exc

    validate_decision(snapshot, decision)
    return decision


def validate_decision(snapshot, decision):
    """拒绝结构不完整或引用未知队列的模型结果。"""
    if not isinstance(decision, dict) or not isinstance(decision.get("queues"), list):
        raise RuntimeError("模型结果格式错误：queues 必须是数组")
    if not isinstance(decision.get("overall_summary"), str):
        raise RuntimeError("模型结果格式错误：缺少 overall_summary")

    expected = {q["name"] for q in snapshot["task_queues"]}
    found = set()
    allowed = {"PURGE_RECOMMEND", "KEEP", "ALERT"}
    for item in decision["queues"]:
        if not isinstance(item, dict) or item.get("queue") not in expected:
            raise RuntimeError("模型结果包含格式错误或未知队列")
        name = item["queue"]
        if name in found:
            raise RuntimeError(f"模型结果重复队列: {name}")
        found.add(name)
        if item.get("decision") not in allowed:
            raise RuntimeError(f"模型结果 decision 无效: {name}")
        confidence = item.get("confidence")
        if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            raise RuntimeError(f"模型结果 confidence 必须在 0 到 1 之间: {name}")
        if not all(isinstance(item.get(key), str) for key in ("reason", "next_action")):
            raise RuntimeError(f"模型结果缺少 reason 或 next_action: {name}")
        if not isinstance(item.get("evidence"), list):
            raise RuntimeError(f"模型结果 evidence 必须是数组: {name}")

    if found != expected:
        raise RuntimeError(f"模型结果队列不完整，缺少: {', '.join(sorted(expected - found))}")


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


def main():
    logger.info("开始 RabbitMQ Agent 巡检")
    stage = "采集队列与日志数据"
    try:
        snapshot = build_snapshot()

        logger.info(
            "发现 %d 个 task 队列，历史数据 %d 天",
            len(snapshot["task_queues"]),
            len(snapshot["daily_history"]),
        )

        stage = "调用模型分析"
        decision = call_llm(snapshot)

        stage = "保存巡检报告"
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
        stage = "发送巡检报告"
        send_wecom(report)

        print(report)
    except Exception as exc:
        logger.exception("巡检在“%s”阶段失败", stage)
        if WECOM_WEBHOOK:
            try:
                send_wecom(build_failure_report(stage, exc))
            except Exception:
                logger.exception("发送巡检失败告警也失败")
        raise


if __name__ == "__main__":
    main()
