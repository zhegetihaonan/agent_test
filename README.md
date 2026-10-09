# RabbitMQ Inspection Agent

这是第一版“观察模式”运维 Agent：

- 每次运行获取 RabbitMQ task 队列状态
- 读取过去最多 3 天 `daily_info.txt`
- 计算任务量/检测率趋势
- 获取 RabbitMQ Docker 容器最近 24 小时日志
- 调用 LLM 做综合判断
- 通过企业微信机器人发送巡检报告
- **不会执行 `purge_queue`**

## 1. 环境要求

服务器需要：

```bash
python3 --version
docker --version
docker exec rabbitmq rabbitmqctl list_queues
```

确认最后一条能正常执行。

## 2. 安装

```bash
cd rabbitmq-inspection-agent

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

如果服务器不能访问外网，需要提前准备 Python 依赖，或者把 pip 源换成你们能访问的内部/国内源。

## 3. 配置

```bash
cp .env.example .env
```

然后设置环境变量。

推荐不要把真实 Key 写进代码。

例如：

```bash
export OPENAI_API_KEY='xxx'
export OPENAI_MODEL='gpt-5.6-luna'
export WECOM_WEBHOOK='https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx'
```

如果你使用 OpenAI 兼容网关：

```bash
export OPENAI_BASE_URL='http://your-gateway/v1'
```

## 4. daily_info.txt

你的现有 Bot 每天追加一行：

```text
2026-10-07,1200000,96.2%
2026-10-08,1100000,93.5%
2026-10-09,1050000,91.8%
```

字段：

```text
日期,任务量,检测率
```

也支持空格/Tab 分隔。

程序每次只取最近 3 条有效数据。

## 5. 手工测试

先不要上 cron。

```bash
source venv/bin/activate

export OPENAI_API_KEY='xxx'
export WECOM_WEBHOOK='xxx'

python3 main.py
```

正常情况下会：

1. 执行 `docker exec rabbitmq rabbitmqctl list_queues name messages consumers`
2. 读取 daily_info.txt
3. 执行 `docker logs --since 24h rabbitmq`
4. 调用 LLM
5. 在 `data/report_YYYYMMDD_HHMMSS.json` 保存完整输入和判断
6. 发企业微信报告

## 6. 非常重要：当前不会清队列

代码里没有调用：

```bash
rabbitmqctl purge_queue
```

即使 Agent 输出：

```text
PURGE_RECOMMEND
```

也只会在企微报告里告诉你建议清理。

## 7. Cron

确认手工运行稳定以后：

```bash
crontab -e
```

例如每天 10:00：

```cron
0 10 * * * cd /opt/rabbitmq-inspection-agent && /opt/rabbitmq-inspection-agent/venv/bin/python3 main.py >> /opt/rabbitmq-inspection-agent/agent.log 2>&1
```

## 8. 后续升级

观察 3~7 天后，可以根据实际判断结果继续增加：

- consumer 数量趋势
- RabbitMQ 消费速率
- queue 增长速度
- 更细的 Docker 日志分析
- Redis/ClickHouse 等业务指标
- 人工确认后再执行 purge
- 最终才考虑完全自动 purge

当前版本的核心目的不是“自动清队列”，而是先验证：

> Agent 根据历史数据 + 当前队列 + 日志做出的判断，是否和人工判断一致。
