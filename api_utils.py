import logging
from datetime import datetime

import pandas as pd
from typing import Dict, List, Optional, Tuple, Any, Callable

logger = logging.getLogger(__name__)
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
    if not models_list:  # 拆分后为空（如models=",,"）
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
            # 转换时间戳为 ES 日期格式（start_time 是秒级时间戳）
            start_date = pd.Timestamp(start_time, unit="s").strftime("%Y-%m-%dT%H:%M:%S")
            time_range["gte"] = start_date
        if end_time:
            end_date = pd.Timestamp(end_time, unit="s").strftime("%Y-%m-%dT%H:%M:%S")
            time_range["lte"] = end_date
        query["bool"]["must"].append({
            "range": {"source.merged_at": time_range}
        })

    # 若没有筛选条件，默认匹配所有
    print(f"*****query is {query}**********************")
    return query if query["bool"]["must"] else {"match_all": {}}


def process_es_commit_response(es_response) -> Dict[str, List[Dict]]:
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
            # 转换为秒级时间戳
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
    seen_pairs: Dict[str, set] = {}  # 存储每个模型下已出现的 (hash, time) 对：{model: {(hash1, time1), (hash2, time2), ...}}

    for item in processed:
        model = item["model_name"]
        # 初始化模型分组和去重集合
        if model not in result:
            result[model] = []
            seen_pairs[model] = set()
        # 生成去重标识（hash+time的元组）
        pair = (item["hash"], item["time"])
        # 若未出现过，则添加到结果
        if pair not in seen_pairs[model]:
            seen_pairs[model].add(pair)
            result[model].append({
                "branch": item["branch"],
                "device": item["device"],
                "hash": item["hash"],
                "time": item["time"]
            })

    return result


def _safe_get(es_source: Dict, key: str, default: Optional[any] = None) -> any:
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
    ES日期字符串转秒级时间戳（处理格式异常）
    :param datetime_str: ES中的日期字符串（如2025-10-22T15:20:00）
    :param fmt: 日期格式
    :return: 时间戳或None
    """
    if not datetime_str:
        return None
    try:
        dt = datetime.strptime(datetime_str, fmt)
        return int(dt.timestamp())
    except ValueError:
        return None


def _format_pair_value(value: Optional[float], default: float = 0.0) -> str:
    """
    格式化“值→值”字符串
    :param value: 要格式化的数值（如吞吐量）
    :param default: 空值时的默认值
    :return: 如“0.36→0.36”的字符串
    """
    safe_val = value if isinstance(value, (int, float)) and value is not None else default
    return f"{safe_val:.2f}→{safe_val:.2f}"


def _process_es_response(
    es_response,
    mapping_func: Callable[[Dict], Dict]  # 接收单条ES source，返回接口格式的函数
) -> List[Dict]:
    """
    批量处理ES响应（提取hits+调用映射函数）
    :param es_response: ES原始响应
    :param mapping_func: 单条数据的映射函数（如map_es_to_response）
    :return: 接口响应列表（无数据返回空列表）
    """
    # 提取ES hits（防御性处理字段缺失）
    es_hits = es_response.get("hits", {}).get("hits", [])
    if not es_hits:
        return []
    # 批量调用映射函数
    return [
        mapping_func(hit.get("_source", {}).get("source", {}))
        for hit in es_hits
    ]


def map_es_to_response(es_source: Dict) -> Dict:
    """模型列表接口：ES数据→接口格式映射（仅保留差异化字段）"""
    return {
        "device": _safe_get(es_source, "device"),
        "latency_s": f"{_convert_ms_to_s(_safe_get(es_source, 'mean_e2el_ms'), 0.0):.2f}→"
                     f"{_convert_ms_to_s(_safe_get(es_source, 'median_e2el_ms'), 0.0):.2f}",
        "mean_itl_ms": _safe_get(es_source, "mean_itl_ms"),
        "mean_tpot_ms": _safe_get(es_source, "mean_tpot_ms"),
        "mean_ttft_ms": _safe_get(es_source, "mean_ttft_ms"),
        "name": _safe_get(es_source, "model_name"),
        "p99_itl_ms": _safe_get(es_source, "p99_itl_ms"),
        "p99_tpot_ms": _safe_get(es_source, "p99_tpot_ms"),
        "p99_ttft_ms": _safe_get(es_source, "p99_ttft_ms"),
        "request_rate": _safe_get(es_source, "request_rate"),
        "requests_req_s": _format_pair_value(_safe_get(es_source, "request_throughput")),
        "serve_output_throughput_tok_s": _safe_get(es_source, "output_token_throughput"),
        "serve_request_throughput_req_s": _safe_get(es_source, "request_throughput"),
        "serve_total_throughput_tok_s": _safe_get(es_source, "total_token_throughput"),
        "tensor_parallel": _safe_get(es_source, "tp"),
        "tokens_tok_s": _format_pair_value(_safe_get(es_source, "total_token_throughput"))
    }


def process_es_model_response(es_response) -> List[Dict]:
    """模型列表接口：批量响应处理"""
    return _process_es_response(es_response, mapping_func=map_es_to_response)


def map_es_to_model_detail(es_source: Dict) -> Dict:
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


def process_es_model_detail_response(es_response) -> List[Dict]:
    """模型详情接口：批量响应处理（"""
    return _process_es_response(es_response, mapping_func=map_es_to_model_detail)
