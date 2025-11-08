import unittest
import json
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List
import sys
from pathlib import Path

# 确保项目根目录在搜索路径中
sys.path.append(str(Path(__file__).parent.parent))

from api_utils import (
    check_input_params, build_es_query, process_data_details_compare_response,
    map_compare_pair_response, _convert_datetime_to_timestamp, _safe_get
)


class TestApiUtils(unittest.TestCase):
    """api_utils.py 完整单元测试（覆盖最新优化逻辑）"""

    # ---------------------- 测试辅助配置 ----------------------
    def setUp(self):
        """初始化测试数据"""
        # 无效值列表（与代码中一致）
        self.invalid_data = ["", "null", None]
        # 有效ES响应模板
        self.valid_es_response = {
            "hits": {
                "hits": [
                    # 有效数据1：Qwen3-8B + tp=1 + request_rate=16 + commit1
                    {"_source": {"source": {
                        "model_name": "Qwen3-8B",
                        "merged_at": "2025-10-02T00:00:00",
                        "request_rate": 16,
                        "commit_id": "commit123",
                        "tp": 1,
                        "mean_e2el_ms": 47.4,
                        "request_throughput": 5.91
                    }}},
                    # 有效数据2：Qwen3-8B + tp=1 + request_rate=16 + commit456
                    {"_source": {"source": {
                        "model_name": "Qwen3-8B",
                        "merged_at": "2025-10-03T00:00:00",
                        "request_rate": 16,
                        "commit_id": "commit456",
                        "tp": 1,
                        "mean_e2el_ms": 54.17,
                        "request_throughput": 5.6
                    }}},
                    # 有效数据3：Llama3-7B + tp=2 + request_rate=32 + commit123
                    {"_source": {"source": {
                        "model_name": "Llama3-7B",
                        "merged_at": "2025-10-02T00:00:00",
                        "request_rate": 32,
                        "commit_id": "commit123",
                        "tp": 2,
                        "mean_e2el_ms": 80.5,
                        "request_throughput": 3.2
                    }}},
                    # 无效数据1：model_name="null"
                    {"_source": {"source": {
                        "model_name": "null",
                        "merged_at": "2025-10-02T00:00:00",
                        "request_rate": 16,
                        "commit_id": "commit123",
                        "tp": 1
                    }}},
                    # 无效数据2：request_rate="null"
                    {"_source": {"source": {
                        "model_name": "Qwen3-8B",
                        "merged_at": "2025-10-02T00:00:00",
                        "request_rate": "null",
                        "commit_id": "commit123",
                        "tp": 1
                    }}},
                    # 无效数据3：tp=None
                    {"_source": {"source": {
                        "model_name": "Qwen3-8B",
                        "merged_at": "2025-10-02T00:00:00",
                        "request_rate": 16,
                        "commit_id": "commit123",
                        "tp": None
                    }}}
                ]
            }
        }

    # ---------------------- 测试 check_input_params ----------------------
    def test_check_input_params_normal(self):
        """正常场景：参数完整有效"""
        raw_params = {
            "startTime": 1730467200,
            "endTime": 1730553600,
            "models": "Qwen3-8B,Llama3-7B",
            "engineVersion": 0,
            "size": 100
        }
        valid, err_msg, params = check_input_params(raw_params)
        self.assertTrue(valid)
        self.assertEqual(params["models"], ["Qwen3-8B", "Llama3-7B"])

    def test_check_input_params_missing_required(self):
        """异常场景：缺失必填参数"""
        raw_params = {
            "startTime": 1730467200,
            "endTime": 1730553600,
            "models": None,
            "engineVersion": 0
        }
        valid, err_msg, _ = check_input_params(raw_params)
        self.assertFalse(valid)
        self.assertIn("缺失必填参数：models", err_msg)

    # ---------------------- 测试 build_es_query ----------------------
    def test_build_es_query_all_params(self):
        """正常场景：传入所有参数，生成完整查询"""
        model_names = ["Qwen3-8B", "Llama3-7B"]
        engine_version = "0"
        start_time = 1730467200
        end_time = 1730553600

        query = build_es_query(model_names, engine_version, start_time, end_time)
        self.assertEqual(len(query["bool"]["must"]), 3)
        self.assertEqual(query["bool"]["must"][0]["terms"], {"source.model_name": model_names})
        self.assertEqual(query["bool"]["must"][1]["term"], {"source.engine_version": "0"})

    # ---------------------- 测试 _convert_datetime_to_timestamp ----------------------
    def test_normal_format_match(self):
        """正常场景：日期字符串完全匹配默认格式（%Y-%m-%dT%H:%M:%S）"""
        datetime_str = "2025-10-02T00:00:00"
        # 计算预期时间戳（UTC时区下该时间的秒级时间戳）
        expected_ts = int(datetime(2025, 10, 2, 0, 0, 0).timestamp())
        # 调用方法
        actual_ts = _convert_datetime_to_timestamp(datetime_str)
        self.assertEqual(actual_ts, expected_ts)

    def test_specified_format_match(self):
        """正常场景：指定自定义格式，日期字符串匹配该格式"""
        # 自定义格式：%Y-%m-%d %H:%M:%S（空格分隔，无T）
        datetime_str = "2025-10-02 08:30:45"
        fmt = "%Y-%m-%d %H:%M:%S"
        # 预期时间戳
        expected_ts = int(datetime(2025, 10, 2, 8, 30, 45).timestamp())
        # 调用方法（指定格式）
        actual_ts = _convert_datetime_to_timestamp(datetime_str, fmt=fmt)
        self.assertEqual(actual_ts, expected_ts)

    def test_format_mismatch_default(self):
        """异常场景：日期字符串与默认格式不匹配（返回None）"""
        # 格式错误：用 / 分隔日期，而非 -
        datetime_str = "2025/10/02T00:00:00"
        actual_ts = _convert_datetime_to_timestamp(datetime_str)
        self.assertIsNone(actual_ts)

    def test_format_mismatch_specified(self):
        """异常场景：日期字符串与指定格式不匹配（返回None）"""
        datetime_str = "2025-10-02T08:30:45"  # 含T
        fmt = "%Y-%m-%d %H:%M:%S"  # 指定格式为空格分隔（无T）
        actual_ts = _convert_datetime_to_timestamp(datetime_str, fmt=fmt)
        self.assertIsNone(actual_ts)

    def test_empty_datetime_str(self):
        """边界场景：日期字符串为空（返回None）"""
        # 测试空字符串、None
        for datetime_str in ["", None]:
            with self.subTest(datetime_str=datetime_str):
                actual_ts = _convert_datetime_to_timestamp(datetime_str)
                self.assertIsNone(actual_ts)

    def test_datetime_with_milliseconds(self):
        """异常场景：日期字符串带毫秒（默认格式不支持，返回None）"""
        datetime_str = "2025-10-02T00:00:00.123"  # 带毫秒
        actual_ts = _convert_datetime_to_timestamp(datetime_str)
        self.assertIsNone(actual_ts)

    def test_datetime_with_timezone(self):
        """异常场景：日期字符串带时区（默认格式不支持，返回None）"""
        datetime_str = "2025-10-02T00:00:00Z"  # 带Z时区
        actual_ts = _convert_datetime_to_timestamp(datetime_str)
        self.assertIsNone(actual_ts)

    # ---------------------- 测试 map_compare_pair_response ----------------------
    def test_map_compare_pair_response_both_data(self):
        """正常场景：新旧数据都存在"""
        old_data = {"model_name": "Qwen3-8B", "tp": 1, "request_rate": 16, "mean_e2el_ms": 47.4}
        new_data = {"model_name": "Qwen3-8B", "tp": 1, "request_rate": 16, "mean_e2el_ms": 54.17}
        result = map_compare_pair_response(old_data, new_data)
        self.assertEqual(result["name"], "Qwen3-8B")
        self.assertEqual(result["tensor_parallel"], "1")
        self.assertEqual(result["request_rate"], 16)
        self.assertEqual(result["latency_s"], "0.05→0.05")

    def test_map_compare_pair_response_one_null(self):
        """异常场景：新数据为None"""
        old_data = {"model_name": "Qwen3-8B", "tp": 1, "request_rate": 16, "mean_e2el_ms": 47.4}
        new_data = None
        result = map_compare_pair_response(old_data, new_data)
        self.assertEqual(result["name"], "Qwen3-8B")
        self.assertEqual(result["tensor_parallel"], "1")
        self.assertEqual(result["latency_s"], "0.05→null")

    def test_map_compare_pair_response_both_null(self):
        """异常场景：新旧数据都为None（生成全null记录）"""
        result = map_compare_pair_response(None, None)
        self.assertEqual(result["name"], "null")
        self.assertEqual(result["tensor_parallel"], "null")
        self.assertEqual(result["request_rate"], None)

    # ---------------------- 测试 process_data_details_compare_response ----------------------
    def test_process_data_details_compare_response_normal(self):
        """正常场景：有效数据生成对比结果，无效数据被过滤"""
        params = {
            "startTime": 1730467200,  # commit123的时间
            "endTime": 1730553600     # commit456的时间
        }

        with patch("api_utils.logger.warning") as mock_warning:
            result = process_data_details_compare_response(self.valid_es_response, params)
            # 验证：有效组合2个（Qwen3-8B+16，Llama3-7B+32），Llama3-7B无end数据→显示null
            self.assertEqual(len(result), 2)
            # 验证Qwen3-8B的结果
            qwen_result = next(item for item in result if item["name"] == "Qwen3-8B")
            self.assertEqual(qwen_result["latency_s"], "0.05→0.05")
            # 验证无效数据被跳过（3条无效数据）
            self.assertEqual(mock_warning.call_count, 3)

    def test_process_data_details_compare_response_filter_all_null(self):
        """异常场景：生成全null记录，被最终过滤"""
        # 模拟ES响应：仅包含1条有效数据（Qwen3-8B + request_rate=16 + commit123）
        es_response = {
            "hits": {
                "hits": [
                    {"_source": {"source": {
                        "model_name": "Qwen3-8B",
                        "merged_at": "2025-10-02T00:00:00",  # time_stamp=1730467200
                        "request_rate": 16,
                        "commit_id": "commit123",
                        "tp": 1,
                        "mean_e2el_ms": 47.4
                    }}}
                ]
            }
        }
        params = {
            "startTime": 1730467200,  # 对应commit123的时间戳
            "endTime": 1730467200 + 3600 * 24 * 7  # 比startTime晚7天，确保无匹配commit
        }

        with patch("api_utils.logger.info") as mock_info, \
                patch("api_utils.logger.warning") as mock_warning:
            # 正常场景：生成1条记录（old_data有，new_data=None，核心字段有效）
            result = process_data_details_compare_response(es_response, params)
            # 验证：未被过滤，长度为1
            self.assertEqual(len(result), 1)
            # 验证核心字段有效（非null/空）
            self.assertEqual(result[0]["name"], "Qwen3-8B")
            self.assertEqual(result[0]["tensor_parallel"], "1")
            self.assertEqual(result[0]["request_rate"], 16)

            # 模拟全null场景：组合为无效模型+无效请求率（无对应数据）
            with patch("api_utils.set") as mock_set:
                # 无效组合：model="invalid" + req_rate=99（data_groups中无该key）
                mock_set.return_value = {("invalid", 99)}
                result_all_null = process_data_details_compare_response(es_response, params)
                self.assertEqual(len(result_all_null), 1)
                # 验证日志输出剔除信息
                self.assertTrue(any("剔除全无效数据" in call[0][0] for call in mock_info.call_args_list))

    def test_process_data_details_compare_response_same_time(self):
        """正常场景：startTime == endTime，对比同一commit"""
        params = {
            "startTime": 1730467200,
            "endTime": 1730467200  # 同一时间戳
        }

        with patch("api_utils.logger.info") as mock_info:
            result = process_data_details_compare_response(self.valid_es_response, params)
            # 验证：Qwen3-8B的对比结果为自我对比
            qwen_result = next(item for item in result if item["name"] == "Qwen3-8B")
            self.assertEqual(qwen_result["latency_s"], "0.05→0.05")

    def test_process_data_details_compare_response_sort(self):
        """正常场景：结果按 model_name→tp→request_rate 排序"""
        params = {
            "startTime": 1730467200,
            "endTime": 1730553600
        }

        result = process_data_details_compare_response(self.valid_es_response, params)
        # 验证排序：Llama3-7B（2）在Qwen3-8B（1）之后
        self.assertEqual(result[0]["name"], "Llama3-7B")
        self.assertEqual(result[1]["name"], "Qwen3-8B")
        # 验证tp排序：Llama3-7B的tp=2 > Qwen3-8B的tp=1
        self.assertEqual(result[0]["tensor_parallel"], "2")
        self.assertEqual(result[1]["tensor_parallel"], "1")

    def test_process_data_details_compare_response_no_valid_data(self):
        """异常场景：无有效数据（全被过滤）"""
        es_response = {
            "hits": {
                "hits": [
                    # 全是无效数据
                    {"_source": {"source": {"model_name": "null", "merged_at": "null", "request_rate": "null", "commit_id": "null", "tp": "null"}}}
                ]
            }
        }
        params = {"startTime": 1730467200, "endTime": 1730553600}

        with patch("api_utils.logger.warning") as mock_warning:
            result = process_data_details_compare_response(es_response, params)
            self.assertEqual(len(result), 0)
            mock_warning.assert_called_with("无有效数据（所有记录均因字段无效被过滤）")


if __name__ == "__main__":
    unittest.main()
