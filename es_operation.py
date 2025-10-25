import threading
from typing import Dict, Optional

from elastic_transport import HeadApiResponse
from elasticsearch import Elasticsearch, exceptions


class ESHandler:
    """Elasticsearch 操作封装类，支持索引管理、数据CRUD及安全锁机制"""

    def __init__(self, es_url: str, username: str, password: str, verify_certs: bool = False):
        """
        初始化ES连接
        :param es_url: ES服务地址（如 "https://localhost:9200"）
        :param username: 登录用户名（默认 "elastic"）
        :param password: 登录密码
        :param verify_certs: 是否验证SSL证书（开发环境建议False）
        """
        self.es = Elasticsearch(
            es_url,
            basic_auth=(username, password),
            verify_certs=verify_certs
        )
        self.lock = threading.Lock()  # 线程锁，保证添加/修改/删除的原子性
        self._check_connection()  # 验证连接是否成功

    def _check_connection(self) -> None:
        """检查ES连接是否正常"""
        try:
            self.es.info()
            print("ES连接成功")
        except exceptions.ConnectionError:
            raise ConnectionError("无法连接到ES服务，请检查地址和端口")
        except exceptions.AuthenticationException:
            raise PermissionError("认证失败，请检查用户名和密码")

    def create_index(self, index_name: str, mappings: Optional[Dict] = None) -> bool:
        """
        创建索引及映射（若索引已存在则不重复创建）
        :param index_name: 索引名称
        :param mappings: 索引映射（结构定义），默认使用模型性能数据的映射
        :return: 创建成功返回True，已存在返回False
        """
        if self.es.indices.exists(index=index_name):
            print(f"⚠索引 '{index_name}' 已存在，无需重复创建")
            return False

        # 默认映射（适配你的模型性能数据结构）
        default_mappings = {
            "properties": {
                "ID": {"type": "keyword"},
                "source": {
                    "properties": {
                        "pr_id": {"type": "keyword"},
                        "commit_id": {"type": "keyword"},
                        "commit_title": {"type": "text"},
                        "created_at": {"type": "text"},
                        "sglang_branch": {"type": "keyword"},
                        "model_name": {"type": "keyword"},
                        "device": {"type": "keyword"},
                        "mean_e2e1_ms": {"type": "float"},
                        "mean_ttft_ms": {"type": "float"},
                        "mean_tpot_ms": {"type": "float"},
                        "mean_itl_ms": {"type": "float"},
                        "p99_e2e1_ms": {"type": "float"},
                        "p99_ttft_ms": {"type": "float"},
                        "p99_tpot_ms": {"type": "float"},
                        "p99_itl_ms": {"type": "float"},
                        "median_e2e1_ms": {"type": "float"},
                        "median_ttft_ms": {"type": "float"},
                        "median_tpot_ms": {"type": "float"},
                        "median_itl_ms": {"type": "float"},
                        "max_concurrency": {"type": "integer"},
                        "request_throughput": {"type": "float"},
                        "total_input_tokens": {"type": "integer"},
                        "total_generated_tokens": {"type": "integer"},
                        "input_token_throughput": {"type": "float"},
                        "output_token_throughput": {"type": "float"},
                        "total_token_throughput": {"type": "float"}
                    }
                }
            }
        }

        # 使用自定义映射（若提供），否则用默认映射
        mappings = mappings or default_mappings
        try:
            self.es.indices.create(index=index_name, mappings=mappings)
            print(f"索引 '{index_name}' 创建成功")
            return True
        except exceptions.RequestError as e:
            print(f"创建索引失败：{e.error}（{e.info}）")
            return False

    def check_id_exists(self, index_name: str, doc_id: str) -> HeadApiResponse | bool:
        """
        检查文档ID是否存在
        :param index_name: 索引名称
        :param doc_id: 文档ID
        :return: 存在返回True，否则False
        """
        try:
            return self.es.exists(index=index_name, id=doc_id)
        except exceptions.RequestError as e:
            print(f"检查ID失败：{e.error}")
            return False

    def add_data(self, index_name: str, doc_id: str, data: Dict) -> bool:
        """
        添加数据（带锁，防止并发写入冲突）
        :param index_name: 索引名称
        :param doc_id: 文档ID（需唯一）
        :param data: 要写入的数据（JSON格式）
        :return: 成功返回True，失败返回False
        """
        with self.lock:  # 加锁保证原子性
            if self.check_id_exists(index_name, doc_id):
                print(f"文档ID '{doc_id}' 已存在，无法重复添加")
                return False
            try:
                response = self.es.index(index=index_name, id=doc_id, document=data)
                if response["result"] == "created":
                    print(f"文档 '{doc_id}' 添加成功")
                    return True
                else:
                    print(f"文档 '{doc_id}' 添加失败：{response['result']}")
                    return False
            except exceptions.RequestError as e:
                print(f"添加数据失败：{e.error}（{e.info}）")
                return False

    def update_data(self, index_name: str, doc_id: str, update_fields: Dict) -> bool:
        """
        修改数据（带锁，只更新指定字段）
        :param index_name: 索引名称
        :param doc_id: 文档ID
        :param update_fields: 要更新的字段（如 {"source.mean_e2e1_ms": 3000.0}）
        :return: 成功返回True，失败返回False
        """
        with self.lock:  # 加锁保证原子性
            if not self.check_id_exists(index_name, doc_id):
                print(f"文档ID '{doc_id}' 不存在，无法修改")
                return False
            try:
                # 使用doc更新指定字段（部分更新）
                response = self.es.update(
                    index=index_name,
                    id=doc_id,
                    doc=update_fields
                )
                if response["result"] in ["updated", "noop"]:  # noop表示无实际修改
                    print(f"文档 '{doc_id}' 更新成功（{response['result']}）")
                    return True
                else:
                    print(f"文档 '{doc_id}' 更新失败：{response['result']}")
                    return False
            except exceptions.RequestError as e:
                print(f"更新数据失败：{e.error}（{e.info}）")
                return False

    def delete_data(self, index_name: str, doc_id: str) -> bool:
        """
        删除数据（带锁，防止并发删除冲突）
        :param index_name: 索引名称
        :param doc_id: 文档ID
        :return: 成功返回True，失败返回False
        """
        with self.lock:  # 加锁保证原子性
            if not self.check_id_exists(index_name, doc_id):
                print(f"文档ID '{doc_id}' 不存在，无法删除")
                return False
            try:
                response = self.es.delete(index=index_name, id=doc_id)
                if response["result"] == "deleted":
                    print(f"文档 '{doc_id}' 删除成功")
                    return True
                else:
                    print(f"文档 '{doc_id}' 删除失败：{response['result']}")
                    return False
            except exceptions.RequestError as e:
                print(f"删除数据失败：{e.error}（{e.info}）")
                return False

    def get_data(self, index_name: str, doc_id: str) -> Optional[Dict]:
        """
        查询单条数据
        :param index_name: 索引名称
        :param doc_id: 文档ID
        :return: 文档数据（_source字段），不存在返回None
        """
        try:
            response = self.es.get(index=index_name, id=doc_id)
            return response["_source"]
        except exceptions.NotFoundError:
            print(f"文档ID '{doc_id}' 不存在")
            return None
        except exceptions.RequestError as e:
            print(f"查询数据失败：{e.error}")
            return None


# 示例用法
if __name__ == "__main__":
    # 配置ES连接信息（替换为你的实际信息）
    ES_CONFIG = {
        "es_url": "https://127.0.0.1:9200",
        "username": "elastic",
        "password": "es30061833",
        "index_name": "sglang_model_performance"
    }

    # 初始化ES处理器
    es_handler = ESHandler(
        es_url=ES_CONFIG["es_url"],
        username=ES_CONFIG["username"],
        password=ES_CONFIG["password"]
    )

    # 1. 创建索引（使用默认映射）
    es_handler.create_index(ES_CONFIG["index_name"])

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
    es_handler.add_data(ES_CONFIG["index_name"], doc_id, test_data)

    # 4. 检查ID是否存在
    print(f"ID存在性检查：{es_handler.check_id_exists(ES_CONFIG['index_name'], doc_id)}")

    # 5. 查询数据
    data = es_handler.get_data(ES_CONFIG["index_name"], doc_id)
    if data:
        print("查询到的数据：", data)

    # 6. 修改数据（示例：更新mean_e2e1_ms字段）
    es_handler.update_data(
        index_name=ES_CONFIG["index_name"],
        doc_id=doc_id,
        update_fields={"source.mean_e2e1_ms": 3000.0}  # 只更新指定字段
    )

    # 7. 再次查询，验证修改结果
    updated_data = es_handler.get_data(ES_CONFIG["index_name"], doc_id)
    if updated_data:
        print("修改后的数据（mean_e2e1_ms）：", updated_data["source"]["mean_e2e1_ms"])

    # 8. 删除数据（如需保留数据，注释此行）
    # es_handler.delete_data(ES_CONFIG["index_name"], doc_id)
