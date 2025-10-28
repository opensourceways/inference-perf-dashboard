import logging
import os

from flask import Flask, request, jsonify
from flask_cors import CORS
from elasticsearch import exceptions
from es_command import es_operation
from api_utils import (
    format_fail,
    check_input_params,
    build_es_query,
    process_es_commit_response
)


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

# 接口定义
@app.route("/health")
def health_check():
    """健康检查接口，同时验证 ES 连接"""
    try:
        es_connected = es_handler and hasattr(es_handler, 'es') and es_handler.es.ping()
    except Exception:
        es_connected = False
    es_status = "connected" if es_connected else "disconnected"
    return jsonify({
        "status": "healthy",
        "es_status": es_status
    }), 200


@app.route("/server/commits/list", methods=["GET"])
def get_server_commits_list():
    """提交列表接口"""
    # 1. ES连接检查
    if not es_handler:
        err_msg = "服务异常：ES连接未就绪"
        logger.error(err_msg)
        return jsonify(format_fail(err_msg)), 500

    try:
        # 2. 提取原始参数
        raw_params = {
            "startTime": request.args.get("startTime", type=int),
            "endTime": request.args.get("endTime", type=int),
            "models": request.args.get("models", type=str),
            "engineVersion": request.args.get("engineVersion", type=int),
            "size": request.args.get("size", type=int)
        }

        # 3. 参数校验（调用工具函数）
        valid, err_msg, params = check_input_params(raw_params)
        if not valid:
            logger.warning(err_msg)
            return jsonify(format_fail(err_msg)), 400

        # 4. 构建ES查询（调用工具函数）
        print("+++++++++++++++4. 构建ES查询（调用工具函数）++++++++++++++++++")
        model_name = None if params["models"] == "all" else params["models"]
        es_query = build_es_query(
            model_name=model_name,
            # engine_version=str(params["engineVersion"]),
            start_time=params["startTime"],
            end_time=params["endTime"]
        )

        # 5. 执行ES查询（新增排序：按创建时间降序，确保最新记录在前）
        print("+++++++++++++++5. 执行ES查询（新增排序：按创建时间降序，确保最新记录在前）++++++++++++++++++")
        es_response = es_handler.search(
            index_name=es_index_name,
            query=es_query,
            size=params["size"],
            sort=[{"source.created_at": {"order": "desc"}}]  # 按时间降序
        )

        # 6. 处理响应数据（调用工具函数）
        print("+++++++++++++++6. 处理响应数据（调用工具函数）++++++++++++++++++")
        result = process_es_commit_response(es_response)

        # 7. 日志+返回结果
        logger.info(f"查询完成：模型数={len(result)}，总记录数={sum(len(v) for v in result.values())}")
        return jsonify(result)

    # 8. 异常处理（分类捕获）
    except ValueError as e:
        err_msg = f"参数错误：{str(e)}"
        logger.error(err_msg)
        return jsonify(format_fail(err_msg)), 400
    except exceptions.RequestError as e:
        err_msg = f"ES查询失败：{e.error}（详情：{e.info.get('error', {}).get('reason', '')}）"
        logger.error(err_msg, exc_info=True)
        return jsonify(format_fail(err_msg)), 500
    except exceptions.ConnectionError:
        err_msg = "ES连接失败：无法连接到ES服务（检查网络或服务状态）"
        logger.error(err_msg, exc_info=True)
        return jsonify(format_fail(err_msg)), 500
    except Exception as e:
        err_msg = f"服务内部错误：{str(e)}"
        logger.error(err_msg, exc_info=True)
        return jsonify(format_fail(err_msg)), 500


if __name__ == "__main__":
    # 启动服务（默认端口5000，可调整）
    app.run(host="0.0.0.0", port=5000, debug=False)