import logging
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from elastic_transport import ObjectApiResponse

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

    # 处理并校验参数
    processed_params = {
        "startTime": params["startTime"],
        "endTime": params["endTime"],
        "models": params["models"].strip(),
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
    model_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None
) -> Dict:
    """
    构建 ES 查询条件（基于数据结构，支持多条件筛选）
    """
    query = {"bool": {"must": []}}  # bool 查询，支持多条件组合

    # 按模型名筛选
    if model_name:
        query["bool"]["must"].append({
            "term": {"source.model_name": model_name}
        })

    # 按source.engine_version筛选
    if engine_version:
        query["bool"]["must"].append({
            "term": {"source.engine_version": engine_version}
        })

    # 按时间范围筛选（source.created_at）
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
            "range": {"source.created_at": time_range}
        })

    # 若没有筛选条件，默认匹配所有
    print(f"*****query is {query}**********************")
    return query if query["bool"]["must"] else {"match_all": {}}


def process_es_commit_response(es_response: ObjectApiResponse[Any]) -> Dict[str, List[Dict]]:
    """
    处理ES提交列表响应，转换为「模型名→记录列表」格式
    """
    # 提取有效记录（过滤字段缺失的数据）
    valid_records: List[Dict] = []
    for hit in es_response.get("hits", {}).get("hits", []):
        source = hit.get("_source", {}).get("source", {})
        required_fields = ["model_name", "sglang_branch", "device", "commit_id", "created_at"]
        if not all(f in source for f in required_fields):
            logger.warning(f"跳过字段缺失的记录（缺少必要字段）：{source}")
            continue
        valid_records.append(source)

    # 转换时间格式+重命名字段
    processed: List[Dict] = []
    for record in valid_records:
        try:
            created_at_dt = pd.to_datetime(record["created_at"], errors="coerce")
            if pd.isna(created_at_dt):
                raise ValueError("无法解析时间格式")
            # 转换为秒级时间戳
            time_stamp = int((created_at_dt - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s"))
            processed.append({
                "model_name": record["model_name"],
                "branch": record["sglang_branch"],
                "device": record["device"],
                "hash": record["commit_id"],
                "time": time_stamp
            })
        except ValueError as e:
            logger.warning(f"时间格式错误（{record['created_at']}）：{str(e)}")
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


def map_es_to_response(es_source: Dict) -> Dict:
    """
    :param es_source: ES查询返回的「_source.source」字典（需包含device、model_name、mean_e2el_ms等字段）
    :type es_source: Dict
    :return: 符合「/server/data-detail-compare/list」接口文档的响应字典
    :rtype: Dict
    """
    # 防御性处理：若es_source为空，返回空字典（避免后续取值报错）
    if not isinstance(es_source, Dict):
        return {}

    # 字段映射：严格对齐接口文档，调用专用辅助函数处理格式化逻辑
    return {
        "device": es_source.get("device"),
        "latency_s": _format_latency(
            mean_e2el=es_source.get("mean_e2el_ms"),
            median_e2el=es_source.get("median_e2el_ms")
        ),
        "mean_itl_ms": es_source.get("mean_itl_ms"),
        "mean_tpot_ms": es_source.get("mean_tpot_ms"),
        "mean_ttft_ms": es_source.get("mean_ttft_ms"),
        "name": es_source.get("model_name"),
        "p99_itl_ms": es_source.get("p99_itl_ms"),
        "p99_tpot_ms": es_source.get("p99_tpot_ms"),
        "p99_ttft_ms": es_source.get("p99_ttft_ms"),
        "request_rate": es_source.get("request_rate"),
        "requests_req_s": _format_throughput(
            value=es_source.get("request_throughput")
        ),
        "serve_output_throughput_tok_s": es_source.get("output_token_throughput"),
        "serve_request_throughput_req_s": es_source.get("request_throughput"),
        "serve_total_throughput_tok_s": es_source.get("total_token_throughput"),
        "tensor_parallel": es_source.get("tp"),
        "tokens_tok_s": _format_throughput(  # 复用吞吐量格式化逻辑（与requests_req_s逻辑一致）
            value=es_source.get("total_token_throughput")
        )
    }


def _format_latency(mean_e2el: Optional[float], median_e2el: Optional[float]) -> str:
    """
    :param mean_e2el: ES中的平均端到端延迟（毫秒），可能为None或非数字
    :type mean_e2el: Optional[float]
    :param median_e2el: ES中的中位数端到端延迟（毫秒），可能为None或非数字
    :type median_e2el: Optional[float]
    :return: 格式化后的延迟字符串（保留2位小数，空值/非数字按0.00处理）
    :rtype: str
    """

    # 处理空值/非数字：转为0.0（避免除法报错）
    def safe_convert(val: Optional[float]) -> float:
        return val / 1000 if (isinstance(val, (int, float)) and val is not None) else 0.0

    # 毫秒转秒 + 保留2位小数 + 拼接「→」格式
    mean_s = safe_convert(mean_e2el)
    median_s = safe_convert(median_e2el)
    return f"{mean_s:.2f}→{median_s:.2f}"


def _format_throughput(value: Optional[float]) -> str:
    """
    :param value: ES中的吞吐量数值（如请求吞吐量、令牌吞吐量），可能为None或非数字
    :type value: Optional[float]
    :return: 格式化后的吞吐量字符串（保留2位小数，空值/非数字按0.00处理）
    :rtype: str
    """
    # 处理空值/非数字：转为0.0
    safe_val = value if (isinstance(value, (int, float)) and value is not None) else 0.0
    # 按接口要求格式返回（当前为「值→值」，后续可扩展对比逻辑）
    return f"{safe_val:.2f}→{safe_val:.2f}"


def process_es_model_response(es_response: ObjectApiResponse[Any]) -> List[Dict]:
    """
    :param es_response: ES查询的原始响应（需包含hits.hits字段）
    :type es_response: ObjectApiResponse[Any]
    :return: 批量映射后的模型列表（无数据时返回空列表）
    :rtype: List[Dict]
    """
    # 提取ES响应中的hits数据（防御性处理：避免字段不存在导致KeyError）
    es_hits = es_response.get("hits", {}).get("hits", [])
    if not es_hits:
        return []

    # 批量调用映射函数，处理每条ES数据
    return [
        map_es_to_response(hit.get("_source", {}).get("source", {}))
        for hit in es_hits
    ]
