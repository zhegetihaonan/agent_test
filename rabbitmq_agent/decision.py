import json
import re

from openai import OpenAI

from .config import OPENAI_API_KEY, OPENAI_BASE_URL, OPENAI_MODEL

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
