import os
import yaml
from elasticsearch import exceptions
from typing import Optional, Tuple
from es_command import es_operation

def init_es_handler(config_path: Optional[str] = None) -> Tuple[Optional[es_operation.ESHandler], str]:
    """
    初始化 ESHandler 实例并返回索引名
    :param config_path: 配置文件路径（默认使用项目内 config/es_config.yaml）
    :return: (es_handler实例, 索引名) → 初始化失败时 es_handler 为 None
    """
    # 1. 确定配置文件路径（默认路径：当前文件目录下的 config/es_config.yaml）
    if not config_path:
        # 获取当前文件的绝对路径目录
        current_file_dir = os.path.dirname(os.path.abspath(__file__))
        # 获取当前文件目录的上一级目录
        parent_dir = os.path.dirname(current_file_dir)  # 向上一级目录
        # 拼接默认配置路径
        config_path = os.path.join(parent_dir, "config", "es_config.yaml")
        # 标准化路径
        config_path = os.path.normpath(config_path)

    # 2. 初始化返回值
    default_index = "sglang_model_performance"

    try:
        # 3. 读取配置文件
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件不存在：{config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            if "es" not in config:
                raise KeyError("配置文件中缺少 'es' 节点")
            es_config = config["es"]

        # 4. 提取 ES 配置参数（带默认值，增强容错）
        es_url = es_config.get("url")
        es_username = es_config.get("username", "elastic")
        es_token = es_config.get("token")
        verify_certs = es_config.get("verify_certs", False)
        index_name = es_config.get("index_name", default_index)

        # 校验必填配置（url 和 token 不可缺失）
        if not es_url:
            raise KeyError("es 配置中缺少 'url' 字段")
        if not es_token:
            raise KeyError("es 配置中缺少 'token' 字段")

        # 5. 初始化 ESHandler 实例
        es_handler = es_operation.ESHandler(
            es_url=es_url,
            username=es_username,
            token=es_token,
            verify_certs=verify_certs
        )

        # 6. 初始化成功，返回实例和索引名
        print(f"ESHandler 初始化成功，索引名：{index_name}")
        return es_handler, index_name

    # 7. 异常处理（分类捕获，明确错误原因）
    except FileNotFoundError as e:
        print(f"配置文件错误：{str(e)}，ES 写入功能禁用")
    except KeyError as e:
        print(f"配置格式错误：{str(e)}，ES 写入功能禁用")
    except (ConnectionError, exceptions.ConnectionError):
        print(f"ES 连接失败：无法连接到 {es_url}，ES 写入功能禁用")
    except (PermissionError, exceptions.AuthenticationException):
        print(f"ES 认证失败：用户名或密码错误，ES 写入功能禁用")
    except Exception as e:
        print(f"ES 初始化异常：{str(e)}，ES 写入功能禁用")

    # 8. 初始化失败时，返回 None 和默认索引名
    return None, default_index

if __name__ == "__main__":
    es_handler, es_index_name = init_es_handler()

    # 1. 创建索引（使用默认映射）
    es_handler.create_index(es_index_name)

    # 2. 准备测试数据
    test_data = {
        "ID": "la2b3c4d_Qwen3-32B",
        "source": {
            "pr_id": "123456",
            "commit_id": "la2b3c4d",
            "commit_title": "This is a test.",
            "created_at": "2025-10-22T15:20:00",
            "sglang_branch": "main",
            "model_name": "Qwen3-32B",
            "device": "Ascend910B3",
            "mean_e2e1_ms": 2801.1999,
            "mean_ttft_ms": 45.0018,
            "mean_tpot_ms": 16.5773,
            "mean_itl_ms": 17.0079,
            "p99_e2e1_ms": 8979.7446,
            "p99_ttft_ms": 48.4629,
            "p99_tpot_ms": 18.222,
            "p99_itl_ms": 19.4617,
            "median_e2e1_ms": 1751.5196,
            "median_ttft_ms": 44.5277,
            "median_tpot_ms": 16.2571,
            "median_itl_ms": 16.445,
            "max_concurrency": 1,
            "request_throughput": 0.357,
            "total_input_tokens": 10759,
            "total_generated_tokens": 1126,
            "input_token_throughput": 548.637,
            "output_token_throughput": 57.4185,
            "total_token_throughput": 606.0555
        }
    }
    doc_id = test_data["ID"]

    # 3. 添加数据
    es_handler.add_data(es_index_name, doc_id, test_data)

    # 4. 检查ID是否存在
    print(f"ID存在性检查：{es_handler.check_id_exists(es_index_name, doc_id)}")

    # 5. 查询数据
    data = es_handler.get_data(es_index_name, doc_id)
    if data:
        print("查询到的数据：", data)

    # 6. 修改数据（示例：更新mean_e2e1_ms字段）
    es_handler.update_data(
        index_name=es_index_name,
        doc_id=doc_id,
        update_fields={"source.mean_e2e1_ms": 3000.0}  # 只更新指定字段
    )

    # 7. 再次查询，验证修改结果
    updated_data = es_handler.get_data(es_index_name, doc_id)
    if updated_data:
        print("修改后的数据（mean_e2e1_ms）：", updated_data["source"]["mean_e2e1_ms"])

    # 8. 删除数据
    # es_handler.delete_data(ES_CONFIG["index_name"], doc_id)