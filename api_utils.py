import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Callable

import pandas as pd

from logger import get_logger
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = get_logger(__name__)

ES_MAX_RESULT_SIZE = 10000


def format_fail(message: str) -> Dict:
    """失败响应格式"""
    return {
        "success": False,
        "data": None,
        "message": message
    }


def check_input_params(params: Dict) -> Tuple[bool, str, Optional[Dict]]:
    """
    校验接口参数
    :return: (校验结果, 错误信息, 处理后参数)
    """
    # 检查必填参数
    required_keys = ["startTime", "endTime", "models", "engineVersion"]
    missing = [k for k in required_keys if k not in params or params[k] is None]
    if missing:
        return False, f"缺失必填参数：{','.join(missing)}", None

    models_list = [m.strip() for m in params["models"].split(",") if m.strip()]
    if not models_list:
        return False, "models参数不可为空（或仅含分隔符）", None

    processed_params = {
        "startTime": params["startTime"],
        "endTime": params["endTime"],
        "models": models_list,
        "engineVersion": params["engineVersion"],
        "size": ES_MAX_RESULT_SIZE if params["size"] is None else params["size"]
    }

    # 校验 engineVersion（仅0/1/2）
    if processed_params["engineVersion"] not in [0, 1, 2]:
        return False, f"engineVersion无效：{processed_params['engineVersion']}，仅支持0/1/2", None

    # 校验时间范围
    if processed_params["startTime"] > processed_params["endTime"]:
        return False, f"时间范围无效：startTime > endTime", None

    # 校验models非空
    if not processed_params["models"]:
        return False, "models参数不可为空", None

    return True, "", processed_params


def build_es_query(
    model_names: Optional[List[str]] = None,
    engine_version: Optional[str] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None
) -> Dict:
    """
    构建 ES 查询条件（基于数据结构，支持多条件筛选）
    """
    query = {"bool": {"must": []}}  # bool 查询，支持多条件组合

    # 按模型名筛选
    if model_names and isinstance(model_names, List) and len(model_names) > 0:
        query["bool"]["must"].append({
            "terms": {"source.model_name": model_names}  # terms 匹配多个模型
        })

    # 按engine_version筛选
    if engine_version:
        query["bool"]["must"].append({
            "term": {"source.engine_version": engine_version}
        })

    # 按时间范围筛选（source.merged_at）
    if start_time or end_time:
        time_range = {}
        if start_time:
            start_date = pd.Timestamp(start_time, unit="s").strftime("%Y-%m-%dT%H:%M:%S")
            time_range["gte"] = start_date
        if end_time:
            end_date = pd.Timestamp(end_time, unit="s").strftime("%Y-%m-%dT%H:%M:%S")
            time_range["lte"] = end_date
        query["bool"]["must"].append({
            "range": {"source.merged_at": time_range}
        })

    return query if query["bool"]["must"] else {"match_all": {}}


def process_commit_response(es_response, params):
    """
    处理ES提交列表响应，转换为「模型名→记录列表」格式
    """
    # 提取有效记录（过滤字段缺失的数据）
    valid_records: List[Dict] = []
    for hit in es_response.get("hits", {}).get("hits", []):
        source = hit.get("_source", {}).get("source", {})
        required_fields = ["model_name", "sglang_branch", "device", "commit_id", "merged_at"]
        if not all(f in source for f in required_fields):
            logger.warning(f"跳过字段缺失的记录（缺少必要字段）：{source}")
            continue
        valid_records.append(source)

    # 转换时间格式+重命名字段
    processed: List[Dict] = []
    for record in valid_records:
        try:
            merged_at_dt = pd.to_datetime(record["merged_at"], errors="coerce")
            if pd.isna(merged_at_dt):
                raise ValueError("无法解析时间格式")
            time_stamp = int((merged_at_dt - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s"))
            processed.append({
                "model_name": record["model_name"],
                "branch": record["sglang_branch"],
                "device": record["device"],
                "hash": record["commit_id"],
                "time": time_stamp
            })
        except ValueError as e:
            logger.warning(f"时间格式错误（{record['merged_at']}）：{str(e)}")
            continue

    # 按模型分组+去重
    result: Dict[str, List[Dict]] = {}
    seen_pairs: Dict[str, set] = {}

    for item in processed:
        model = item["model_name"]
        if model not in result:
            result[model] = []
            seen_pairs[model] = set()
        pair = (item["hash"], item["time"])
        if pair not in seen_pairs[model]:
            seen_pairs[model].add(pair)
            result[model].append({
                "branch": item["branch"],
                "device": item["device"],
                "hash": item["hash"],
                "time": item["time"]
            })

    return result


def _safe_get(es_source: Dict, key: str, default: Optional[any] = None):
    """
    安全从ES source中取值（避免KeyError，处理空值）
    :param es_source: ES的_source.source字典
    :param key: 要获取的字段名
    :param default: 默认值（默认None）
    :return: 字段值或默认值
    """
    if not isinstance(es_source, Dict):
        return default
    return es_source.get(key, default)


def _convert_ms_to_s(ms_value: Optional[float], default: Optional[float] = None) -> Optional[float]:
    """
    毫秒转秒（处理空值/非数字，保留2位小数）
    :param ms_value: 毫秒值（如mean_e2el_ms）
    :param default: 转换失败时的默认值
    :return: 秒值或默认值
    """
    if not isinstance(ms_value, (int, float)) or ms_value is None:
        return default
    return round(ms_value / 1000, 2)


def _convert_datetime_to_timestamp(datetime_str: Optional[str], fmt: str = "%Y-%m-%dT%H:%M:%S") -> Optional[int]:
    """
    ES日期字符串转秒级时间戳
    :param datetime_str: ES中的日期字符串（如2025-10-22T15:20:00）
    :return: 时间戳或None
    """
    if not datetime_str:
        return None
    try:
        dt = datetime.strptime(datetime_str, fmt)
        return int(dt.timestamp())
    except ValueError:
        return None


def _process_compare_response(
    es_response,
    mapping_func: Callable[[Dict], Dict]  # 接收单条ES source，返回接口格式的函数
) -> List[Dict]:
    """
    批量处理ES响应
    :param es_response: ES原始响应
    :param mapping_func: 单条数据的映射函数
    :return: 接口响应列表
    """
    es_hits = es_response.get("hits", {}).get("hits", [])
    if not es_hits:
        return []
    return [
        mapping_func(hit.get("_source", {}).get("source", {}))
        for hit in es_hits
    ]


def map_compare_pair_response(old_data: Optional[Dict], new_data: Optional[Dict]) -> Dict:
    """
    双时间点数据对比：旧数据（commit1）+ 新数据（commit2）→ 接口格式（旧→新）
    """
    def _format_pair(old_val, new_val) -> str:
        def _safe_format(val):
            if val is None or not isinstance(val, (int, float)):
                return "null"
            return f"{val:.2f}"

        return f"{_safe_format(old_val)}→{_safe_format(new_val)}"

    def _get_single_value(key: str) -> str:
        old_val = _safe_get(old_data, key) if old_data else None
        new_val = _safe_get(new_data, key) if new_data else None
        # 优先取旧值，旧值为空则取新值，都为空则返回null
        val = old_val if (old_val is not None and isinstance(old_val, (int, float))) else new_val
        if val is None or not isinstance(val, (int, float)):
            return "null"
        return f"{val:.0f}" if isinstance(val, int) else f"{val:.2f}"

    # 基础字段：模型名和设备（取非空值）
    def _get_base_field(key: str) -> str:
        old_val = _safe_get(old_data, key) if old_data else None
        new_val = _safe_get(new_data, key) if new_data else None
        return str(old_val) if old_val is not None else (str(new_val) if new_val is not None else "null")

    return {
        "name": _get_base_field("model_name"),
        "tensor_parallel": _get_single_value("tp"),
        "request_rate": int(_get_single_value("request_rate")) if _get_single_value("request_rate") != "null" else None,
        "device": _get_base_field("device"),
        "latency_s": _format_pair(
            _convert_ms_to_s(_safe_get(old_data, "mean_e2el_ms")) if old_data else None,
            _convert_ms_to_s(_safe_get(new_data, "mean_e2el_ms")) if new_data else None
        ),
        "mean_itl_ms": _format_pair(
            _safe_get(old_data, "mean_itl_ms") if old_data else None,
            _safe_get(new_data, "mean_itl_ms") if new_data else None
        ),
        "mean_tpot_ms": _format_pair(
            _safe_get(old_data, "mean_tpot_ms") if old_data else None,
            _safe_get(new_data, "mean_tpot_ms") if new_data else None
        ),
        "mean_ttft_ms": _format_pair(
            _safe_get(old_data, "mean_ttft_ms") if old_data else None,
            _safe_get(new_data, "mean_ttft_ms") if new_data else None
        ),
        "p99_itl_ms": _format_pair(
            _safe_get(old_data, "p99_itl_ms") if old_data else None,
            _safe_get(new_data, "p99_itl_ms") if new_data else None
        ),
        "p99_tpot_ms": _format_pair(
            _safe_get(old_data, "p99_tpot_ms") if old_data else None,
            _safe_get(new_data, "p99_tpot_ms") if new_data else None
        ),
        "p99_ttft_ms": _format_pair(
            _safe_get(old_data, "p99_ttft_ms") if old_data else None,
            _safe_get(new_data, "p99_ttft_ms") if new_data else None
        ),
        # 吞吐量指标对比
        "serve_request_throughput_req_s": _format_pair(
            _safe_get(old_data, "request_throughput") if old_data else None,
            _safe_get(new_data, "request_throughput") if new_data else None
        ),
        "serve_output_throughput_tok_s": _format_pair(
            _safe_get(old_data, "output_token_throughput") if old_data else None,
            _safe_get(new_data, "output_token_throughput") if new_data else None
        ),
        "serve_total_throughput_tok_s": _format_pair(
            _safe_get(old_data, "total_token_throughput") if old_data else None,
            _safe_get(new_data, "total_token_throughput") if new_data else None
        ),
        "requests_req_s": _format_pair(
            _safe_get(old_data, "request_throughput") if old_data else None,
            _safe_get(new_data, "request_throughput") if new_data else None
        ),
        "tokens_tok_s": _format_pair(
            _safe_get(old_data, "total_token_throughput") if old_data else None,
            _safe_get(new_data, "total_token_throughput") if new_data else None
        )
    }


def process_data_details_compare_response(es_response, params) -> List[Dict]:
    """
    处理双时间点对比响应
    :param es_response: ES原始响应
    :param params: 包含startTime（commit1）和endTime（commit2）的参数
    :return: 对比格式的结果列表
    """
    # 提取有效数据（含commit_id，校验核心字段）
    valid_data: List[Dict] = []
    invalid_data = ["", "null", None]
    for hit in es_response.get("hits", {}).get("hits", []):
        source = hit.get("_source", {}).get("source", {})

        # 提取核心字段并过滤无效值（"null"/空字符串/非数字等）
        model_name = _safe_get(source, "model_name")
        if model_name in invalid_data:
            logger.warning(f"跳过无效model_name：{model_name}")
            continue

        merged_at = _safe_get(source, "merged_at")
        if merged_at in invalid_data:
            logger.warning(f"跳过无效merged_at：{merged_at}")
            continue

        request_rate = _safe_get(source, "request_rate")
        if request_rate in invalid_data:
            logger.warning(f"跳过无效request_rate：{request_rate}")
            continue

        try:
            request_rate_float = float(request_rate)
        except (ValueError, TypeError):
            logger.warning(f"跳过无法转换为数字的request_rate：{request_rate}")
            continue

        if not request_rate_float.is_integer():
            logger.warning(f"request_rate不是整数：{request_rate}（值为{request_rate_float}）")
            continue

        # 转换为整数
        request_rate = int(request_rate_float)

        commit_id = _safe_get(source, "commit_id")
        if commit_id in invalid_data:
            logger.warning(f"跳过无效commit_id：{commit_id}")
            continue

        tp = _safe_get(source, "tp")
        if tp in invalid_data or not isinstance(tp, (int, float)):
            logger.warning(f"跳过无效tp：{tp}")
            continue

        # 转换时间戳（必须有效）
        time_stamp = _convert_datetime_to_timestamp(merged_at)
        if not time_stamp:
            logger.warning(f"时间转换失败：merged_at={merged_at}")
            continue

        # 仅保留完全有效的数据
        valid_data.append({
            **source,
            "time_stamp": time_stamp,
            "commit_id": commit_id,
            "request_rate": request_rate,
            "tp_int": int(tp)
        })

    if not valid_data:
        logger.warning("无有效数据（所有记录均因字段无效被过滤）")
        return []

    # 确定目标commit_id
    target_start = params["startTime"]
    target_end = params["endTime"]

    start_commit = min(valid_data, key=lambda x: abs(x["time_stamp"] - target_start))["commit_id"]
    end_commit = start_commit if target_start == target_end else \
    min(valid_data, key=lambda x: abs(x["time_stamp"] - target_end))["commit_id"]
    logger.info(f"目标对比commit：start={start_commit}，end={end_commit}")

    # 按（model, request_rate, commit_id）分组
    data_groups: Dict[Tuple[str, int, str], List[Dict]] = {}
    for data in valid_data:
        key = (data["model_name"], data["request_rate"], data["commit_id"])
        if key not in data_groups:
            data_groups[key] = []
        data_groups[key].append(data)

    # 提取有效（model, request_rate）组合（基于有效数据）
    all_combinations = set()
    for (model, req_rate, commit) in data_groups.keys():
        # 再次过滤模型名为无效值的组合
        if model not in invalid_data:
            all_combinations.add((model, req_rate))

    if not all_combinations:
        logger.warning("无有效（model, request_rate）组合")
        return []

    # 生成对比结果
    result: List[Dict] = []
    for (model, req_rate) in all_combinations:
        start_key = (model, req_rate, start_commit)
        old_data = data_groups[start_key][0] if start_key in data_groups else None

        end_key = (model, req_rate, end_commit)
        new_data = data_groups[end_key][0] if end_key in data_groups else None

        compare_result = map_compare_pair_response(old_data, new_data)
        result.append(compare_result)

    # 过滤全无效数据（name/tp/request_rate均为无效值）
    filtered_result = []
    for item in result:
        # 判定无效值："null"/空字符串
        is_name_invalid = item["name"] in invalid_data
        is_tp_invalid = item["tensor_parallel"] in invalid_data
        is_req_rate_invalid = item["request_rate"] in invalid_data

        if is_name_invalid and is_tp_invalid and is_req_rate_invalid:
            logger.info(f"剔除全无效数据：{item}")
            continue
        filtered_result.append(item)

    result_sorted = sorted(
        filtered_result,
        key=lambda x: (
            x["name"] if x["name"] not in invalid_data else float("inf"),
            float(x["tensor_parallel"]) if x["tensor_parallel"] not in invalid_data else float("inf"),
            float(x["request_rate"]) if x["request_rate"] not in invalid_data else float("inf")
        )
    )

    logger.info(f"处理完成：原始{len(result)}条，过滤后{len(filtered_result)}条有效数据")
    return result_sorted


def map_data_details(es_source: Dict) -> Dict:
    """模型详情接口：ES数据→接口格式映射"""
    return {
        "time": _convert_datetime_to_timestamp(_safe_get(es_source, "merged_at")),
        "model_name": _safe_get(es_source, "model_name"),
        "hash": _safe_get(es_source, "commit_id"),
        "status": _safe_get(es_source, "status"),
        "requests_per_second": _safe_get(es_source, "request_throughput"),
        "tokens_per_second": _safe_get(es_source, "total_token_throughput"),
        "qps": _safe_get(es_source, "request_rate"),
        "mean_itl_ms": _safe_get(es_source, "mean_itl_ms"),
        "mean_tpot_ms": _safe_get(es_source, "mean_tpot_ms"),
        "mean_ttft_ms": _safe_get(es_source, "mean_ttft_ms"),
        "p99_itl_ms": _safe_get(es_source, "p99_itl_ms"),
        "p99_tpot_ms": _safe_get(es_source, "p99_tpot_ms"),
        "p99_ttft_ms": _safe_get(es_source, "p99_ttft_ms"),
        "request_throughput_serve_per_sec": _safe_get(es_source, "request_throughput"),
        "output_throughput_serve_per_sec": _safe_get(es_source, "output_token_throughput"),
        "total_token_throughput_per_sec": _safe_get(es_source, "total_token_throughput"),
        "latency": _convert_ms_to_s(_safe_get(es_source, "mean_e2el_ms"))
    }


def process_data_details_response(es_response, params) -> List[Dict]:
    """模型详情接口：批量响应处理（"""
    return _process_compare_response(es_response, mapping_func=map_data_details)
