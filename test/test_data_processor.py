import unittest
import os
import json
import tempfile
from dataclasses import fields
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path

# 确保项目根目录在搜索路径中
import sys
sys.path.append(str(Path(__file__).parent))

from data.data_processor import (
    parse_metrics_csv, parse_metrics_json, parse_pr_json, merge_metrics,
    check_model_files, get_date_str, generate_single_model_data,
    ROOT_DIR, METRIC_CSV_DIR, METRIC_JSON_DIR, PR_INFO_DIR
)
from data.data_models import Metric, PRInfo


class TestDataProcessor(unittest.TestCase):
    """data_processor.py 核心函数单元测试"""

    # ---------------------- 测试辅助：创建临时文件 ----------------------
    def setUp(self):
        """测试前创建临时目录和测试文件"""
        # 临时目录（模拟 ROOT_DIR/日期/commit/model/request_rate 结构）
        self.temp_root = tempfile.TemporaryDirectory()
        self.test_date = "20251022"
        self.test_commit = "abc123"
        self.test_model = "Qwen3-8B"
        self.test_request_rate = "16"

        # 构建临时文件路径
        self.temp_model_dir = os.path.join(
            self.temp_root.name, self.test_date, self.test_commit, self.test_model, self.test_request_rate
        )
        os.makedirs(self.temp_model_dir, exist_ok=True)

        # 临时 PR JSON 路径
        self.temp_pr_json = os.path.join(
            self.temp_root.name, self.test_date, self.test_commit, PR_INFO_DIR
        )

    def tearDown(self):
        """测试后清理临时目录"""
        self.temp_root.cleanup()

    # ---------------------- 测试 parse_metrics_csv ----------------------
    def test_parse_metrics_csv_normal(self):
        """正常场景：CSV 格式正确，解析成功"""
        # 创建测试 CSV 内容（移除每行的前导空格，确保Stage值为纯"total"）
        csv_content = """Stage,Performance Parameters,Average,Median,P99
total,E2EL,47.4 ms,54.17 ms,366.74 ms
total,TTFT,185.57 ms,303.94 ms,716.8 ms
total,TPOT,73.05 ms,100.85 ms,224.22 ms
total,ITL,47.4 ms,54.17 ms,366.74 ms
total,InputTokens,1000.0,1000.0,1000.0
total,OutputTokens,2000.0,2000.0,2000.0"""

        # 写入临时 CSV 文件
        temp_csv = os.path.join(self.temp_model_dir, METRIC_CSV_DIR)
        with open(temp_csv, "w", encoding="utf-8") as f:
            f.write(csv_content)

        # 执行解析
        result = parse_metrics_csv(temp_csv, stage="total")

        # 验证结果（核心字段是否存在且格式正确）
        self.assertIsInstance(result, dict)
        self.assertEqual(result["mean_e2el_ms"], 47.4)
        self.assertEqual(result["median_ttft_ms"], 303.94)
        self.assertEqual(result["total_input_tokens"], 1000.0)
        self.assertEqual(len(result), 14)  # 12个延迟字段 + 2个token字段

    def test_parse_metrics_csv_missing_stage(self):
        """异常场景：CSV 中无指定 stage，抛出 ValueError"""
        csv_content = """Stage,Performance Parameters,Average,test,E2EL,47.4 ms"""
        temp_csv = os.path.join(self.temp_model_dir, METRIC_CSV_DIR)
        with open(temp_csv, "w", encoding="utf-8") as f:
            f.write(csv_content)

        with self.assertRaises(ValueError) as ctx:
            parse_metrics_csv(temp_csv, stage="total")
        self.assertIn("stage='total'", str(ctx.exception))

    def test_parse_metrics_csv_missing_fields(self):
        """异常场景：CSV 缺失必需字段，抛出 ValueError"""
        csv_content = """Stage,Performance Parameters,Average,total,E2EL,47.4 ms"""  # 缺少 TPOT/ITL 等参数
        temp_csv = os.path.join(self.temp_model_dir, METRIC_CSV_DIR)
        with open(temp_csv, "w", encoding="utf-8") as f:
            f.write(csv_content)

        with self.assertRaises(ValueError) as ctx:
            parse_metrics_csv(temp_csv, stage="total")
        self.assertIn("CSV中未找到stage='total'的数据", str(ctx.exception))

    # ---------------------- 测试 parse_metrics_json ----------------------
    def test_parse_metrics_json_normal(self):
        """正常场景：JSON 格式正确，解析成功"""
        json_content = {
            "Max Concurrency": {"total": 8},
            "Request Throughput": {"total": "5.91 req/s"},
            "Total Input Tokens": {"total": 10000},
            "Total generated tokens": {"total": 20000},
            "Input Token Throughput": {"total": "1200 token/s"},
            "Output Token Throughput": {"total": "1321.33 token/s"},
            "Total Token Throughput": {"total": "2609.05 token/s"},
            "tp": {"total": 1},
            "request_rate": {"total": 16}
        }
        temp_json = os.path.join(self.temp_model_dir, METRIC_JSON_DIR)
        with open(temp_json, "w", encoding="utf-8") as f:
            json.dump(json_content, f)

        result = parse_metrics_json(temp_json, stage="total")

        self.assertIsInstance(result, dict)
        self.assertEqual(result["max_concurrency"], 8)
        self.assertEqual(result["request_throughput"], 5.91)
        self.assertEqual(result["tp"], 1)
        self.assertEqual(result["request_rate"], 16)

    def test_parse_metrics_json_invalid_format(self):
        """异常场景：JSON 格式错误，抛出 ValueError"""
        temp_json = os.path.join(self.temp_model_dir, METRIC_JSON_DIR)
        with open(temp_json, "w", encoding="utf-8") as f:
            f.write("{invalid json}")  # 非法 JSON

        with self.assertRaises(ValueError) as ctx:
            parse_metrics_json(temp_json)
        self.assertIn("JSON格式错误", str(ctx.exception))

    # ---------------------- 测试 parse_pr_json ----------------------
    def test_parse_pr_json_normal(self):
        """正常场景：PR JSON 格式正确，解析成功"""
        pr_content = {
            "pr_id": "PR123",
            "commit_id": "abc123456",
            "pr_title": "优化推理性能",
            "merged_at": "2025-10-22T14:51:00",
            "sglang_branch": "main",
            "device": "Altlas A2"
        }
        with open(self.temp_pr_json, "w", encoding="utf-8") as f:
            json.dump(pr_content, f)

        pr_info, commit_id = parse_pr_json(self.temp_pr_json)

        self.assertIsInstance(pr_info, PRInfo)
        self.assertEqual(pr_info.pr_id, "PR123")
        self.assertEqual(commit_id, "abc123456")
        self.assertEqual(pr_info.device, "Altlas A2")

    def test_parse_pr_json_missing_fields(self):
        """异常场景：PR JSON 缺失必填字段，抛出 ValueError"""
        pr_content = {
            "pr_id": "PR123",
            "commit_id": "abc123456",
            # 缺少 merged_at 字段
            "sglang_branch": "main",
            "device": "Altlas A2"
        }
        with open(self.temp_pr_json, "w", encoding="utf-8") as f:
            json.dump(pr_content, f)

        with self.assertRaises(ValueError) as ctx:
            parse_pr_json(self.temp_pr_json)
        self.assertIn("缺少必填字段", str(ctx.exception))

    def test_parse_pr_json_invalid_merged_at(self):
        """异常场景：merged_at 格式错误，抛出 ValueError"""
        pr_content = {
            "pr_id": "PR123",
            "commit_id": "abc123456",
            "pr_title": "优化推理性能",
            "merged_at": "2025-10-22 14:51:00",  # 错误格式（缺少T）
            "sglang_branch": "main",
            "device": "Altlas A2"
        }
        with open(self.temp_pr_json, "w", encoding="utf-8") as f:
            json.dump(pr_content, f)

        with self.assertRaises(ValueError) as ctx:
            parse_pr_json(self.temp_pr_json)
        self.assertIn("merged_at格式错误", str(ctx.exception))

    # ---------------------- 测试 merge_metrics ----------------------
    def test_merge_metrics_normal(self):
        """正常场景：CSV 和 JSON 指标合并成功"""
        csv_metrics = {
            "mean_e2el_ms": 47.4,
            "median_e2el_ms": 54.17,
            "total_input_tokens": 1000.0,
            "total_generated_tokens": 2000.0
        }
        json_metrics = {
            "max_concurrency": 8,
            "request_throughput": 5.91,
            "tp": 1,
            "request_rate": 16,
            "model_name": "Qwen3-8B",
            "status": "normal",
            "engine_version": "0"
        }

        # 补全所有 Metric 必需字段（避免缺失）
        all_csv_fields = {field.name for field in fields(Metric) if field.name in csv_metrics}
        missing_csv_fields = {field.name for field in fields(Metric)} - all_csv_fields - set(json_metrics.keys())
        for field in missing_csv_fields:
            csv_metrics[field] = 0.0  # 填充默认值

        result = merge_metrics(csv_metrics, json_metrics)

        self.assertIsInstance(result, dict)
        self.assertEqual(result["model_name"], "Qwen3-8B")
        self.assertEqual(result["request_throughput"], 5.91)
        self.assertEqual(result["mean_e2el_ms"], 47.4)
        # 验证所有 Metric 字段都存在
        self.assertTrue(all(field.name in result for field in fields(Metric)))

    # ---------------------- 测试 check_model_files ----------------------
    def test_check_model_files_all_exist(self):
        """正常场景：所有文件（CSV/JSON/PR JSON）都存在"""
        # 创建3个必需文件
        temp_csv = os.path.join(self.temp_model_dir, METRIC_CSV_DIR)
        temp_json = os.path.join(self.temp_model_dir, METRIC_JSON_DIR)
        with open(temp_csv, "w") as f:
            f.write("test")
        with open(temp_json, "w") as f:
            f.write("{}")
        with open(self.temp_pr_json, "w") as f:
            f.write("{}")

        # 替换 ROOT_DIR 为临时目录
        with patch("data.data_processor.ROOT_DIR", self.temp_root.name):
            is_valid, missing_files, file_paths = check_model_files(
                self.test_date, self.test_commit, self.test_model, self.test_request_rate
            )

        self.assertTrue(is_valid)
        self.assertEqual(len(missing_files), 0)
        self.assertTrue(os.path.exists(file_paths["csv_path"]))
        self.assertTrue(os.path.exists(file_paths["pr_json_path"]))

    def test_check_model_files_missing_csv(self):
        """异常场景：缺失 CSV 文件"""
        # 只创建 JSON 和 PR JSON
        temp_json = os.path.join(self.temp_model_dir, METRIC_JSON_DIR)
        with open(temp_json, "w") as f:
            f.write("{}")
        with open(self.temp_pr_json, "w") as f:
            f.write("{}")

        with patch("data.data_processor.ROOT_DIR", self.temp_root.name):
            is_valid, missing_files, _ = check_model_files(
                self.test_date, self.test_commit, self.test_model, self.test_request_rate
            )

        self.assertFalse(is_valid)
        self.assertEqual(len(missing_files), 1)
        self.assertIn("csv：", missing_files[0])

    # ---------------------- 测试 get_date_str ----------------------
    def test_get_date_str_with_param(self):
        """正常场景：传入合法日期字符串"""
        date_str = "20251023"
        result = get_date_str(date_str)
        self.assertEqual(result, date_str)

    def test_get_date_str_invalid_param(self):
        """异常场景：传入非法日期字符串"""
        with self.assertRaises(ValueError) as ctx:
            get_date_str("20251301")  # 13月无效
        self.assertIn("格式错误，应为YYYYMMDD", str(ctx.exception))

    def test_get_date_str_default(self):
        """正常场景：未传入日期，返回前一天日期"""
        result = get_date_str()
        self.assertEqual(len(result), 8)  # 格式 YYYYMMDD
        self.assertIsInstance(int(result), int)  # 纯数字

    # ---------------------- 测试 generate_single_model_data ----------------------
    @patch("data.data_processor.batch_create_metrics_data")
    def test_generate_single_model_data_normal(self, mock_batch):
        """正常场景：单模型数据生成成功"""
        # Mock 批量生成函数返回值
        mock_batch.return_value = [{"ID": "abc123_Qwen3-8B_16", "source": {}}]

        # 准备文件路径
        file_paths = {
            "csv_path": "fake_csv.csv",
            "metrics_json_path": "fake_json.json",
            "pr_json_path": "fake_pr.json"
        }

        result = generate_single_model_data("Qwen3-8B", file_paths)

        self.assertIsInstance(result, dict)
        self.assertEqual(result["ID"], "abc123_Qwen3-8B_16")
        mock_batch.assert_called_once()  # 验证批量函数被调用

    @patch("data.data_processor.batch_create_metrics_data")
    def test_generate_single_model_data_empty(self, mock_batch):
        """异常场景：批量生成返回空列表"""
        mock_batch.return_value = []

        file_paths = {
            "csv_path": "fake_csv.csv",
            "metrics_json_path": "fake_json.json",
            "pr_json_path": "fake_pr.json"
        }

        with self.assertRaises(Exception) as ctx:
            generate_single_model_data("Qwen3-8B", file_paths)
        self.assertIn("无有效数据生成", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
