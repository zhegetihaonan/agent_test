import re
import subprocess
from datetime import datetime
from pathlib import Path

from .config import (
    CONTAINER_NAME,
    DAILY_INFO_FILE,
    DOCKER_LOG_HOURS,
    DOCKER_LOG_MAX_CHARS,
    logger,
)


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
