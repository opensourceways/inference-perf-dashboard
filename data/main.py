from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import os

app = FastAPI(title="模型性能数据接口", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境替换为具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


METRICS_FILE_PATH = "metrics_total.json"  # 数据文件路径
# 响应字段映射：直接关联source中的真实字段
FIELD_MAPPING = {
    "branch": "pr_branch",  # 响应branch → source.pr_branch
    "device": "device",  # 响应device → source.device
    "hash": "commit_id",  # 响应hash → source.commit_id（新增字段直接复用）
    "time": "pr_timestamp"  # 响应time → 精确到秒的时间戳（pr_date+pr_time计算）
}

# /server/data-details/list 接口的响应字段映射
DETAILS_FIELD_MAPPING = {
    "time": ("pr_timestamp", 0),  # 时间戳（复用预处理的pr_timestamp）
    "model_name": ("model_name", "unknown_model"),  # 模型名
    "hash": ("commit_id", "unknown_hash"),  # commit哈希（对应source.commit_id）
    "status": ("status", "normal"),  # 状态（原始数据无，默认normal）
    "requests_per_second": ("request_throughput", 0.0),  # 请求吞吐量 → 响应QPS
    "tokens_per_second": ("total_token_throughput", 0.0),  # 总token吞吐量 → 响应总token/s
    "qps": ("request_throughput", 0.0),  # QPS（复用请求吞吐量，若原始数据有单独qps字段可修改）
    "mean_itl_ms": ("avg_itl", 0.0),  # 平均token间隔延迟 → 响应mean_itl_ms
    "mean_tpot_ms": ("avg_tpot", 0.0),  # 平均每个输出token时间 → 响应mean_tpot_ms
    "mean_ttft_ms": ("avg_ttft", 0.0),  # 平均首token时间 → 响应mean_ttft_ms
    "p99_itl_ms": ("p99_itl", 0.0),  # P99 token间隔延迟 → 响应p99_itl_ms
    "p99_tpot_ms": ("p99_tpot", 0.0),  # P99 输出token时间 → 响应p99_tpot_ms
    "p99_ttft_ms": ("p99_ttft", 0.0),  # P99 首token时间 → 响应p99_ttft_ms
    "request_throughput_serve_per_sec": ("request_throughput", 0.0),  # 服务请求吞吐量（复用）
    "output_throughput_serve_per_sec": ("output_token_throughput", 0.0),  # 输出token吞吐量
    "total_token_throughput_per_sec": ("total_token_throughput", 0.0),  # 总token吞吐量
    "latency": ("avg_e2el", 0.0)  # 延迟（用平均端到端延迟avg_e2el代替，可根据实际调整）
}

def load_and_preprocess_metrics() -> List[Dict[str, Any]]:
    if not os.path.exists(METRICS_FILE_PATH):
        raise FileNotFoundError(f"性能数据文件不存在：{METRICS_FILE_PATH}")

    # 解析JSON（兼容数组/单条数据）
    try:
        with open(METRICS_FILE_PATH, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
            raw_data = [raw_data] if isinstance(raw_data, dict) else raw_data
    except json.JSONDecodeError as e:
        raise ValueError(f"metrics.json格式错误：{str(e)}")

    processed_data = []
    for item in raw_data:
        source = item.get("source", {})
        # 获取source中的commit_id（无需拆分ID，避免错误）
        commit_id = source.get("commit_id", "unknown_commit")

        # 计算精确时间戳（pr_date+pr_time → 如"2025-10-22 15:20:11"）
        pr_date_str = source.get("pr_date", "")
        pr_time_str = source.get("pr_time", "00:00:00")  # 默认值
        pr_timestamp = 0
        try:
            if pr_date_str:
                # 合并日期和时间，转为秒级时间戳
                full_datetime_str = f"{pr_date_str} {pr_time_str}"
                full_datetime = datetime.strptime(full_datetime_str, "%Y-%m-%d %H:%M:%S")
                pr_timestamp = int(full_datetime.timestamp())
        except ValueError:
            pr_timestamp = 0  # 格式错误时设为0，后续过滤排除

        # 预处理字段
        processed_item = {
            "raw": item,
            "commit_id": commit_id,
            "pr_timestamp": pr_timestamp,
            "model_name": source.get("model_name", "unknown_model"),
            "engine_version": source.get("engineVersion", 0)  # 预留版本字段
        }
        processed_data.append(processed_item)

    return processed_data


def filter_data(
        data: List[Dict[str, Any]],
        start_time: int,
        end_time: int,
        models: str,
        engine_version: int
) -> List[Dict[str, Any]]:
    filtered = []
    for item in data:
        # 时间范围过滤（精确到秒）
        if not (start_time <= item["pr_timestamp"] <= end_time):
            continue
        # 引擎版本过滤（预留）
        if item["engine_version"] != engine_version:
            continue
        # 模型过滤（all=所有，否则匹配具体模型）
        if models != "all" and item["model_name"] != models:
            continue
        filtered.append(item)
    return filtered


# ---------------------- 接口1：/server/commits/list ----------------------
@app.get("/server/commits/list", summary="按模型分组获取commit列表")
def get_commits_list(
        start_time: int = Query(..., description="开始时间戳（秒级），示例：1750687211（2025-10-22 15:20:11）"),
        end_time: int = Query(..., description="结束时间戳（秒级），示例：1750690811"),
        models: str = Query("all", description="模型名（all=所有，或具体模型如DeepSeek-V3.2）"),
        engine_version: int = Query(0, description="引擎版本（0/1/2，预留）")
) -> dict[str, str] | dict[str, list[dict[str, Any]]]:
    try:
        processed_data = load_and_preprocess_metrics()
    except (FileNotFoundError, ValueError) as e:
        return {"error": str(e)}

    filtered_data = filter_data(processed_data, start_time, end_time, models, engine_version)
    if not filtered_data:
        return {"message": "未找到匹配数据"}

    # 按模型分组，直接从source取commit_id（无冗余计算）
    result: Dict[str, List[Dict[str, Any]]] = {}
    for item in filtered_data:
        model_name = item["model_name"]
        source = item["raw"]["source"]

        commit_info = {
            "branch": source.get(FIELD_MAPPING["branch"], "unknown_branch"),
            "device": source.get(FIELD_MAPPING["device"], "unknown_device"),
            "hash": source.get(FIELD_MAPPING["hash"], "unknown_hash"),  # 直接用source.commit_id
            "time": item[FIELD_MAPPING["time"]]  # 精确时间戳
        }

        if model_name not in result:
            result[model_name] = []
        result[model_name].append(commit_info)

    return result


# ---------------------- 接口2：/server/data-detail-compare/list ----------------------
@app.get("/server/data-detail-compare/list", summary="获取单个模型的详细性能数据")
def get_model_detail_list(
        start_time: int = Query(..., description="开始时间戳（秒级），示例：1750687211"),
        end_time: int = Query(..., description="结束时间戳（秒级），示例：1750690811"),
        models: str = Query(..., description="具体模型名（如DeepSeek-V3.2，不支持all）"),
        engine_version: int = Query(0, description="引擎版本（0/1/2，预留）")
) -> dict[str, str] | list[Any]:
    # 禁止models=all，确保返回单个模型数据
    if models == "all":
        return {"error": "该接口仅支持单个模型查询，请指定具体模型名（如DeepSeek-V3.2）"}

    try:
        processed_data = load_and_preprocess_metrics()
    except (FileNotFoundError, ValueError) as e:
        return {"error": str(e)}

    filtered_data = filter_data(processed_data, start_time, end_time, models, engine_version)
    if not filtered_data:
        return {"message": "未找到匹配数据"}

    # 格式化详细数据：包含ID+所有source字段（性能指标+PR信息）
    result = []
    for item in filtered_data:
        raw = item["raw"]
        detail = {
            "ID": raw.get("ID", "unknown_id"),  # 保留原始ID
            **raw.get("source", {})  # 展开所有source字段（含commit_id、性能指标）
        }
        result.append(detail)

    return result

# ---------------------- 接口3：/server/data-details/list（获取model详情数据列表） ----------------------
@app.get("/server/data-details/list", summary="获取单个模型的详情数据列表（含性能指标）")
def get_model_details_list(
        start_time: int = Query(..., description="开始时间戳（秒级），示例：1750687211（2025-10-22 15:20:11）"),
        end_time: int = Query(..., description="结束时间戳（秒级），示例：1750687220"),
        models: str = Query(..., description="具体模型名（如DeepSeek-V3.2，不支持all）"),
        engine_version: int = Query(0, description="引擎版本（0/1/2，预留）")
) -> dict[str, str] | list[Any]:
    """
    接口功能：返回单个模型的详细性能详情列表，包含QPS、延迟、吞吐量等核心指标
    响应格式：按请求参数筛选后，返回符合格式的性能详情数组
    """
    # 校验models参数（禁止all，仅支持单个模型）
    if models == "all":
        return {"error": "该接口仅支持单个模型查询，请指定具体模型名（如DeepSeek-V3.2）"}

    # 加载并预处理数据
    try:
        processed_data = load_and_preprocess_metrics()
    except (FileNotFoundError, ValueError) as e:
        return {"error": str(e)}

    # 按参数过滤数据
    filtered_data = filter_data(processed_data, start_time, end_time, models, engine_version)
    if not filtered_data:
        return {"message": "未找到匹配的模型详情数据"}

    # 格式化响应（按DETAILS_FIELD_MAPPING映射，确保格式正确）
    result = []
    for item in filtered_data:
        source = item["raw"]["source"]  # 原始数据的source字段
        detail_item = {}

        # 遍历字段映射，生成响应数据（自动填充默认值）
        for resp_field, (raw_field, default_val) in DETAILS_FIELD_MAPPING.items():
            # 优先从预处理数据取（如time对应pr_timestamp），其次从source取，最后用默认值
            if resp_field == "time":
                detail_item[resp_field] = item.get(raw_field, default_val)
            else:
                detail_item[resp_field] = source.get(raw_field, default_val)

        result.append(detail_item)

    return result

# ---------------------- 启动服务器 ----------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app="main:app", host="127.0.0.1", port=8000, reload=True)