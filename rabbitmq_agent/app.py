import json
from datetime import datetime
from pathlib import Path

from .analysis import build_snapshot
from .config import WECOM_WEBHOOK, logger
from .decision import call_llm
from .reporting import (
    build_failure_report,
    build_wecom_markdown,
    send_wecom,
)


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
