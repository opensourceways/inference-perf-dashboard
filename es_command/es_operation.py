import threading
from typing import Dict, Optional, Any

from elastic_transport import HeadApiResponse, ObjectApiResponse
from elasticsearch import Elasticsearch, exceptions, logger

class ESHandler:
    """Elasticsearch 操作封装类，支持索引管理、数据CRUD及安全锁机制"""

    # 默认映射（适配模型性能数据结构）
    DEFAULT_MAPPINGS = {
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

    def __init__(self, es_url: str, username: str, token: str, verify_certs: bool = False):
        """
        初始化ES连接
        :param es_url: ES服务地址（如 "https://localhost:9200"）
        :param username: 登录用户名（默认 "elastic"）
        :param token: 登录密码
        :param verify_certs: 是否验证SSL证书
        """
        self.es = Elasticsearch(
            es_url,
            basic_auth=(username, token),
            verify_certs=verify_certs
        )
        self.lock = threading.Lock()  # 线程锁，保证添加/修改/删除的原子性
        self._check_connection()  # 验证连接是否成功

    @classmethod
    def update_default_mappings(cls, new_mappings: Dict) -> None:
        """
        类方法：更新默认映射（会影响所有实例的默认映射）
        :param new_mappings: 新的默认映射（完整结构）
        """
        cls.DEFAULT_MAPPINGS = new_mappings
        print("默认映射已更新")

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
            print(f"索引 '{index_name}' 已存在，无需重复创建")
            return False


        # 使用自定义映射（若提供），否则用默认映射
        mappings = mappings or self.DEFAULT_MAPPINGS
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

    def search(self, index_name: str, query: Dict, size: int = 10000) -> ObjectApiResponse[Any]:
        """
        执行批量查询（支持条件筛选）
        :param index_name: 索引名称
        :param query: 查询条件（ES 语法）
        :param size: 返回数量
        :return: ES 原始响应
        """
        try:
            return self.es.search(
                index=index_name,
                query=query,
                size=size
            )
        except exceptions.RequestError as e:
            logger.error(f"批量查询失败：{e.error}（{e.info}）")
            raise  # 抛出异常由调用方处理
