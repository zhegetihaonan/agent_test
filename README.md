# RabbitMQ Inspection Agent

一个用于个人业务巡检的轻量 Agent：汇总 RabbitMQ task 队列、最近 7 天任务量与检出率、容器日志，由代码规则和 LLM 共同形成关注建议，并可发送到企业微信。

当前是观察模式，只提供建议，不执行 `purge_queue`。

## 运行

需要 Python、Docker，以及可执行 `docker exec rabbitmq rabbitmqctl list_queues` 的环境。

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

在 `.env` 中填写 `OPENAI_API_KEY` 和 `WECOM_WEBHOOK`，然后运行：

```bash
./run.sh
```

`data/daily_info.txt` 每行格式为 `日期,任务量,检出率`，例如 `2026-10-14,760000,11.2%`。仓库里的记录是演示数据，接入业务时可替换为实际数据。也可以加入crontab，每天定时生成报告。

## 判断与留档

代码负责计算趋势并标出明确规则命中（例如有积压但消费者为 0）；LLM 结合日志和业务趋势解释原因、给出下一步建议。模型输出会做基本结构校验。每次巡检输入与判断会保存到 `data/report_*.json`，企微消息格式示例如下。

```text
RabbitMQ Agent 巡检报告
检查时间：2026-10-14 10:00:00
阈值参考：500,000 条

过去七天趋势
- 任务量趋势：stable（2.4%）
- 检出率趋势：falling（-1.6 个百分点）

数据采集
- 已读取 7 条历史记录
- RabbitMQ 日志：840 行，关键词命中 3 行

队列情况
- task_queue：620,000 条，消费者 8，建议 ALERT，置信度 0.86
  - 原因：积压超过参考阈值，且检出率近七天走低
  - 建议：检查消费速率及近期检出链路变更，观察积压是否持续增长
  - 证据：积压高于 500,000；消费者数为 8；检出率下降 1.6 个百分点

总结
建议先检查检出链路与消费速度，当前证据不足以支持清队列。

当前为观察模式：Agent 只提供建议，不执行 `purge_queue`。
```
