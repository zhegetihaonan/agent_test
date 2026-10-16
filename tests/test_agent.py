import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from rabbitmq_agent import analysis, collectors, reporting
from rabbitmq_agent import decision as llm


class AgentChecks(unittest.TestCase):
    def test_load_daily_info_keeps_latest_seven_valid_rows(self):
        rows = [f"2026-10-{day:02d},{500000 + day},10.{day % 10}%" for day in range(1, 10)]
        with tempfile.TemporaryDirectory() as directory:
            data_file = Path(directory) / "daily_info.txt"
            data_file.write_text("\n".join(rows), encoding="utf-8")
            with patch.object(collectors, "DAILY_INFO_FILE", str(data_file)):
                history = collectors.load_daily_info()

        self.assertEqual(len(history), 7)
        self.assertEqual(history[0]["date"], "2026-10-03")
        self.assertEqual(history[-1]["date"], "2026-10-09")

    def test_validate_decision_rejects_missing_queue_result(self):
        snapshot = {"task_queues": [{"name": "task_a"}]}
        result = {"queues": [], "overall_summary": "summary"}

        with self.assertRaises(RuntimeError):
            llm.validate_decision(snapshot, result)

    def test_send_wecom_retries_transient_request_failure(self):
        response = Mock()
        response.json.return_value = {"errcode": 0}
        with (
            patch.object(reporting, "WECOM_WEBHOOK", "https://wecom.example/hook"),
            patch.object(
                reporting.requests,
                "post",
                side_effect=[reporting.requests.ConnectionError("temporary"), response],
            ) as post,
            patch.object(reporting.time, "sleep") as sleep,
        ):
            reporting.send_wecom("report")

        self.assertEqual(post.call_count, 2)
        sleep.assert_called_once_with(1)

    def test_zero_baseline_trends_are_explicit(self):
        no_tasks = analysis.calculate_trends([
            {"tasks": 0, "detect_rate": 0},
            {"tasks": 0, "detect_rate": 0},
        ])
        started = analysis.calculate_trends([
            {"tasks": 0, "detect_rate": 0},
            {"tasks": 100, "detect_rate": 10},
        ])

        self.assertEqual(no_tasks["task_trend"], "no_tasks")
        self.assertEqual(no_tasks["task_change_pct"], None)
        self.assertEqual(no_tasks["task_change_absolute"], 0)
        self.assertEqual(started["task_trend"], "started_from_zero")
        self.assertEqual(started["task_change_pct"], None)
        self.assertEqual(started["task_change_absolute"], 100)
        self.assertIn("从零开始产生任务", reporting.format_task_trend(started))
        self.assertNotIn("%", reporting.format_task_trend(started))

    def test_positive_baseline_can_fall_to_zero(self):
        trend = analysis.calculate_trends([
            {"tasks": 100, "detect_rate": 10},
            {"tasks": 0, "detect_rate": 8},
        ])

        self.assertEqual(trend["task_trend"], "falling")
        self.assertEqual(trend["task_change_pct"], -100)
        self.assertEqual(trend["task_change_absolute"], -100)

    def test_single_history_row_is_insufficient(self):
        trend = analysis.calculate_trends([{"tasks": 0, "detect_rate": 0}])

        self.assertEqual(trend["task_trend"], "insufficient_data")
        self.assertIsNone(trend["task_change_pct"])
        self.assertIsNone(trend["task_change_absolute"])

    def test_docker_logs_merge_streams_and_mark_failures_unavailable(self):
        outputs = (
            "stdout line\nERROR stdout",
            "ERROR stderr",
            "stdout warning\nERROR stderr",
        )
        for output in outputs:
            with self.subTest(output=output):
                result = Mock(returncode=0, stdout=output)
                with patch.object(collectors.subprocess, "run", return_value=result) as run:
                    logs = collectors.get_docker_logs()

                self.assertTrue(logs["available"])
                self.assertGreaterEqual(logs["abnormal_line_count"], 1)
                self.assertEqual(run.call_args.kwargs["stderr"], subprocess.STDOUT)

        failed = Mock(returncode=1, stdout="docker daemon unavailable")
        with patch.object(collectors.subprocess, "run", return_value=failed):
            logs = collectors.get_docker_logs()

        self.assertFalse(logs["available"])
        self.assertEqual(logs["exit_code"], 1)
        self.assertIsNone(logs["abnormal_line_count"])
        self.assertIn("daemon unavailable", logs["error"])

    def test_unavailable_logs_are_reported_as_alert(self):
        with (
            patch.object(analysis, "list_queues", return_value=[]),
            patch.object(analysis, "load_daily_info", return_value=[]),
            patch.object(
                analysis,
                "get_docker_logs",
                return_value=collectors.unavailable_docker_logs("docker logs failed", 1),
            ),
        ):
            snapshot = analysis.build_snapshot()

        report = reporting.build_wecom_markdown(
            snapshot,
            {"queues": [], "overall_summary": "日志不可用，无法判断"},
        )
        self.assertTrue(any(item["level"] == "ALERT" for item in snapshot["rule_findings"]))
        self.assertIn("RabbitMQ 容器日志不可用", report)
        self.assertIn("ALERT", report)


if __name__ == "__main__":
    unittest.main()
