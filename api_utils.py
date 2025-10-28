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
    # 1. 检查必填参数
    required_keys = ["startTime", "endTime", "models", "engineVersion"]
    missing = [k for k in required_keys if k not in params or params[k] is None]
    if missing:
        return False, f"缺失必填参数：{','.join(missing)}", None

    # 2. 处理并校验参数
    processed_params = {
        "startTime": params["startTime"],
        "endTime": params["endTime"],
        "models": params["models"].strip(),
        "engineVersion": params["engineVersion"],
        "size": min(params.get("size", 10000), ES_MAX_RESULT_SIZE)  # 限制最大条数
    }

    # 3. 校验 engineVersion（仅0/1/2）
    if processed_params["engineVersion"] not in [0, 1, 2]:
        return False, f"engineVersion无效：{processed_params['engineVersion']}，仅支持0/1/2", None

    # 4. 校验时间范围
    if processed_params["startTime"] > processed_params["endTime"]:
        return False, f"时间范围无效：startTime > endTime", None

    # 5. 校验models非空
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
    构建 ES 查询条件（基于你的数据结构，支持多条件筛选），数据中时间是字符串格式（"2025-10-22T15:20:00"），需转换为时间戳范围查询
    """
    query = {"bool": {"must": []}}  # bool 查询，支持多条件组合

    # 按模型名筛选（source.model_name）
    if model_name:
        query["bool"]["must"].append({
            "match": {"source.model_name": model_name}
        })

    # 按source.engine_version筛选
    if engine_version:
        query["bool"]["must"].append({
            "match": {"source.engine_version": engine_version}
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
    return query if query["bool"]["must"] else {"match_all": {}}

def process_es_commit_response(es_response: ObjectApiResponse[Any]) -> Dict[str, List[Dict]]:
    """
    处理ES提交列表响应，转换为「模型名→记录列表」格式
    :param es_response: ES查询原始响应
    :return: 按模型分组的结果
    """
    # 1. 提取有效记录（过滤字段缺失的数据）
    valid_records: List[Dict] = []
    for hit in es_response.get("hits", {}).get("hits", []):
        source = hit.get("_source", {}).get("source", {})
        required_fields = ["model_name", "sglang_branch", "device", "commit_id", "created_at"]
        if not all(f in source for f in required_fields):
            logger.warning(f"跳过字段缺失的记录：{source}")
            continue
        valid_records.append(source)

    # 2. 转换时间格式+重命名字段
    processed: List[Dict] = []
    for record in valid_records:
        try:
            # 字符串时间→秒级时间戳
            created_at_dt = pd.to_datetime(record["created_at"], format="%Y-%m-%dT%H:%M:%S")
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

    # 3. 按模型分组+去重
    result: Dict[str, List[Dict]] = {}
    for item in processed:
        model = item.pop("model_name")  # 移除临时模型名
        # 初始化分组
        if model not in result:
            result[model] = []
        # 去重：避免同一模型下相同hash+time的记录
        if not any(r["hash"] == item["hash"] and r["time"] == item["time"] for r in result[model]):
            result[model].append(item)

    return result
