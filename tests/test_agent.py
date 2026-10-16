import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import main


class AgentChecks(unittest.TestCase):
    def test_load_daily_info_keeps_latest_seven_valid_rows(self):
        rows = [f"2026-10-{day:02d},{500000 + day},10.{day % 10}%" for day in range(1, 10)]
        with tempfile.TemporaryDirectory() as directory:
            data_file = Path(directory) / "daily_info.txt"
            data_file.write_text("\n".join(rows), encoding="utf-8")
            with patch.object(main, "DAILY_INFO_FILE", str(data_file)):
                history = main.load_daily_info()

        self.assertEqual(len(history), 7)
        self.assertEqual(history[0]["date"], "2026-10-03")
        self.assertEqual(history[-1]["date"], "2026-10-09")

    def test_validate_decision_rejects_missing_queue_result(self):
        snapshot = {"task_queues": [{"name": "task_a"}]}
        decision = {"queues": [], "overall_summary": "summary"}

        with self.assertRaises(RuntimeError):
            main.validate_decision(snapshot, decision)

    def test_send_wecom_retries_transient_request_failure(self):
        response = Mock()
        response.json.return_value = {"errcode": 0}
        with (
            patch.object(main, "WECOM_WEBHOOK", "https://wecom.example/hook"),
            patch.object(
                main.requests,
                "post",
                side_effect=[main.requests.ConnectionError("temporary"), response],
            ) as post,
            patch.object(main.time, "sleep") as sleep,
        ):
            main.send_wecom("report")

        self.assertEqual(post.call_count, 2)
        sleep.assert_called_once_with(1)


if __name__ == "__main__":
    unittest.main()
