import logging
import os
from typing import Dict, Optional, List

import pandas as pd
import yaml
from flask import Flask, request, jsonify
from flask_cors import CORS
from elasticsearch import exceptions

from es_command import es_operation

# ------------------------------
# 基础配置与初始化
# ------------------------------
# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 移除代理环境变量
for proxy in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
    os.environ.pop(proxy, None)

# Flask 应用初始化
app = Flask(__name__)
CORS(app)

# 初始化 ESHandler 实例（全局复用，避免重复连接）
es_handler, es_index_name = es_operation.init_es_handler()

# ------------------------------
# 数据处理工具函数
# ------------------------------
def format_response(data: Dict) -> Dict:
    """格式化响应数据（统一结构，便于前端处理）"""
    return {
        "success": True,
        "data": data,
        "message": "查询成功"
    }


def format_error(message: str) -> Dict:
    """格式化错误响应"""
    return {
        "success": False,
        "data": None,
        "message": message
    }


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


# ------------------------------
# 接口定义（基于你的数据结构）
# ------------------------------
@app.route("/health")
def health_check():
    """健康检查接口，同时验证 ES 连接"""
    es_status = "connected" if (es_handler and hasattr(es_handler, 'es') and es_handler.es.ping()) else "disconnected"
    return jsonify({
        "status": "healthy",
        "es_status": es_status
    }), 200

@app.route("/server/commits/list", methods=["GET"])
def get_server_commits_list():
    """
    获取提交列表接口（复用 es_operation 已有工具，严格匹配需求）
    接口地址：/server/commits/list?models=all&startTime={startTime}&endTime={endTime}&engineVersion={engineVersion}
    """
    # 1. 前置检查：ES 连接是否就绪
    if not es_handler:
        logger.error("ESHandler 未初始化，无法执行查询")
        return jsonify(format_error("服务异常：ES 连接未就绪")), 500

    try:
        # 2. 提取并校验请求参数（严格匹配需求定义）
        # 2.1 提取参数（指定类型，缺失则返回400）
        params = {
            "startTime": request.args.get("startTime", type=int),
            "endTime": request.args.get("endTime", type=int),
            "models": request.args.get("models", type=str),
            "engineVersion": request.args.get("engineVersion", type=int)
        }

        # 2.2 校验必填参数（startTime/endTime/engineVersion/models 不可缺失）
        missing_params = [k for k, v in params.items() if v is None]
        if missing_params:
            err_msg = f"缺失必填参数：{','.join(missing_params)}"
            logger.warning(err_msg)
            return jsonify(format_error(err_msg)), 400

        # 2.3 校验参数合法性
        # 2.3.1 engineVersion 仅支持 0/1/2
        if params["engineVersion"] not in [0, 1, 2]:
            err_msg = f"engineVersion 无效：{params['engineVersion']}，仅支持 0/1/2"
            logger.warning(err_msg)
            return jsonify(format_error(err_msg)), 400

        # 2.3.2 时间范围合理性（startTime ≤ endTime）
        if params["startTime"] > params["endTime"]:
            err_msg = f"时间范围无效：startTime({params['startTime']}) > endTime({params['endTime']})"
            logger.warning(err_msg)
            return jsonify(format_error(err_msg)), 400

        # 2.3.3 models 不为空字符串
        if not params["models"].strip():
            err_msg = "models 参数不可为空"
            logger.warning(err_msg)
            return jsonify(format_error(err_msg)), 400

        # 3. 构建 ES 查询条件（复用 es_operation 已有的 build_es_query 函数）
        # 3.1 处理 models 参数：models=all → 不筛选模型；否则按模型名筛选
        model_name = None if params["models"].strip() == "all" else params["models"].strip()
        # 3.2 调用已有工具函数构建查询（engineVersion 转字符串，匹配 build_es_query 入参类型）
        es_query = build_es_query(
            model_name=model_name,
            engine_version=str(params["engineVersion"]),  # 适配 build_es_query 的 str 类型
            start_time=params["startTime"],
            end_time=params["endTime"]
        )

        # 4. 执行 ES 查询（调用扩展后的 search 方法，指定排序和字段过滤）
        # 4.1 定义排序规则：按创建时间降序（最新提交在前）
        sort_rule = [{"source.created_at": {"order": "desc"}}]
        # 4.2 定义需返回的字段（仅取接口需要的字段，减少数据传输）
        need_fields = [
            "source.model_name",
            "source.sglang_branch",
            "source.device",
            "source.commit_id",
            "source.created_at"
        ]
        # 4.3 调用 ESHandler.search（复用已有实例，传递扩展参数）
        es_response = es_handler.search(
            index_name=es_index_name,
            query=es_query,
            size=min(int(request.args.get("size", 10000)), 10000),  # 限制最大返回量
        )

        # 5. 处理 ES 响应数据（按需求格式转换）
        # 5.1 提取有效记录（过滤字段缺失的无效数据）
        valid_records: List[Dict] = []
        for hit in es_response.get("hits", {}).get("hits", []):
            # 从响应中提取嵌套的 source 数据（你的数据结构：_source → source）
            source_data = hit.get("_source", {}).get("source", {})
            # 校验关键字段是否存在（避免后续处理报错）
            required_source_fields = ["model_name", "sglang_branch", "device", "commit_id", "created_at"]
            if not all(field in source_data for field in required_source_fields):
                logger.debug(f"跳过字段缺失的记录：{source_data}")
                continue
            valid_records.append(source_data)

        # 5.2 转换数据格式（时间戳、字段重命名）
        processed_records: List[Dict] = []
        for record in valid_records:
            try:
                # 转换 created_at（字符串）→ time（秒级时间戳）
                created_at_dt = pd.to_datetime(
                    record["created_at"],
                    format="%Y-%m-%dT%H:%M:%S",  # 匹配你的时间格式
                    errors="raise"
                )
                time_stamp = int((created_at_dt - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s"))

                # 重命名字段（匹配响应要求：sglang_branch→branch，commit_id→hash）
                processed_records.append({
                    "model_name": record["model_name"],  # 用于后续分组
                    "branch": record["sglang_branch"],
                    "device": record["device"],
                    "hash": record["commit_id"],
                    "time": time_stamp
                })
            except ValueError as e:
                logger.warning(f"处理记录失败（created_at格式错误）：{record['created_at']}，错误：{str(e)}")
                continue

        # 5.3 按模型名分组（生成需求格式：模型名→列表）
        commit_result: Dict[str, List[Dict]] = {}
        for record in processed_records:
            model = record["model_name"]
            # 移除临时的 model_name 字段，避免响应中多余
            record.pop("model_name")

            # 初始化模型分组（首次出现该模型时）
            if model not in commit_result:
                commit_result[model] = []

            # 去重：避免同一模型下出现相同 hash+time 的重复记录
            if not any(
                    item["hash"] == record["hash"] and item["time"] == record["time"]
                    for item in commit_result[model]
            ):
                commit_result[model].append(record)

        # 6. 返回最终响应（严格匹配需求格式：无外层包裹，模型名→列表）
        logger.info(
            f"查询完成：模型数={len(commit_result)}，总记录数={len(processed_records)}，"
            f"条件：models={params['models']}, engineVersion={params['engineVersion']}, "
            f"时间范围={params['startTime']}-{params['endTime']}"
        )
        return jsonify(commit_result)

    # 7. 异常捕获与分类处理（复用已有 format_error 函数）
    except ValueError as e:
        # 参数格式错误（如时间转换失败）
        err_msg = f"参数处理错误：{str(e)}"
        logger.error(err_msg)
        return jsonify(format_error(err_msg)), 400
    except exceptions.RequestError as e:
        # ES 查询语法/配置错误
        err_msg = f"数据查询失败：{e.error}（详情：{e.info.get('error', {}).get('reason', '')}）"
        logger.error(err_msg, exc_info=True)
        return jsonify(format_error(err_msg)), 500
    except Exception as e:
        # 其他未知异常
        err_msg = f"服务内部错误：{str(e)}"
        logger.error(err_msg, exc_info=True)
        return jsonify(format_error(err_msg)), 500


if __name__ == "__main__":
    # 启动服务（默认端口5000，可调整）
    app.run(host="0.0.0.0", port=5000, debug=False)