import logging
import os
from typing import Dict, List, Callable, Any

from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from elasticsearch import exceptions
from es_command import es_operation
from api_utils import (
    format_fail,
    check_input_params,
    build_es_query,
    process_es_commit_response,
    process_es_model_response,
    process_es_model_detail_response
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

def es_api_handler(
    # 差异化逻辑：由具体接口传入
    adjust_params: Callable[[Dict], Dict],  # 调整参数
    process_response: Callable[[Any], Any],  # 响应处理
    format_log: Callable[[Dict, Any], str]   # 日志格式化
) -> Callable[[], Response]:
    """
    封装ES接口的公共流程，返回具体接口函数 ES连接检查 → 提取参数 → 参数校验 → 调整参数 → 构建查询 → 执行查询 → 处理响应 → 日志 → 返回结果
    """
    def api_func() -> tuple[Response, int] | Response:
        # ES连接检查
        if not es_handler:
            err_msg = "服务异常：ES连接未就绪"
            logger.error(err_msg)
            return jsonify(format_fail(err_msg)), 500

        try:
            # 提取原始参数 统一提取接口的公共参数，差异部分由adjust_params处理
            raw_params = {
                "startTime": request.args.get("startTime", type=int),
                "endTime": request.args.get("endTime", type=int),
                "models": request.args.get("models", type=str),
                "engineVersion": request.args.get("engineVersion", default=0, type=int),
                "size": request.args.get("size", type=int)
            }

            # 参数校验
            valid, err_msg, params = check_input_params(raw_params)
            if not valid:
                logger.warning(err_msg)
                return jsonify(format_fail(err_msg)), 400

            # 调整参数
            adjusted_params = adjust_params(params)

            # 构建ES查询
            es_query = build_es_query(
                model_names=adjusted_params["model_names"],
                engine_version=str(adjusted_params["engineVersion"]),
                start_time=adjusted_params["startTime"],
                end_time=adjusted_params["endTime"]
            )

            # 执行ES查询
            es_response = es_handler.search(
                index_name=es_index_name,
                query=es_query,
                size=adjusted_params["size"],
                sort=None
            )

            # 处理响应（不同接口用不同process函数）
            result = process_response(es_response)

            # 格式化日志（不同接口日志内容不同）
            log_msg = format_log(adjusted_params, result)
            logger.info(log_msg)

            # 返回结果
            return jsonify(result)

        # 统一异常处理
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

    return api_func


# 提交列表接口专用函数
def adjust_commit_params(params: Dict) -> Dict:
    model_names = None if params["models"] == ["all"] else params["models"]
    return {**params, "model_names": model_names}


def format_commit_log(params: Dict, result: Dict) -> str:
    return f"查询完成：模型数={len(result)}，总记录数={sum(len(v) for v in result.values())}"


# 模型列表接口专用函数
def adjust_model_list_params(params: Dict) -> Dict:
    return {**params, "model_names": params["models"]}


def format_model_list_log(params: Dict, result: List[Dict]) -> str:
    return (f"模型列表查询完成：返回模型数={len(result)}，查询条件=models={params['model_names']}, "
            f"engineVersion={params['engineVersion']}")


# 模型详情接口专用函数（已按你的格式）
def adjust_model_detail_params(params: Dict) -> Dict:
    return {**params, "model_names": params["models"]}


def format_model_detail_log(params: Dict, result: List[Dict]) -> str:
    return (f"模型详情查询完成：返回数据条数={len(result)}，"
            f"查询条件=models={params['model_names']}, engineVersion={params['engineVersion']}, "
            f"时间范围={params['startTime']}~{params['endTime']}")


# 路由注册（直接返回es_api_handler结果）
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
    return es_api_handler(
        adjust_params=adjust_commit_params,
        process_response=process_es_commit_response,
        format_log=format_commit_log
    )()


@app.route("/server/data-details-compare/list", methods=["GET"])
def get_server_model_list():
    return es_api_handler(
        adjust_params=adjust_model_list_params,
        process_response=process_es_model_response,
        format_log=format_model_list_log
    )()


@app.route("/server/data-details/list", methods=["GET"])
def get_server_model_detail_list():
    return es_api_handler(
        adjust_params=adjust_model_detail_params,
        process_response=process_es_model_detail_response,
        format_log=format_model_detail_log
    )()


if __name__ == "__main__":
    # 启动服务（默认端口5000，可调整）
    app.run(host="0.0.0.0", port=5000, debug=False)
