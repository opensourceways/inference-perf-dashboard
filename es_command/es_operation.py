import os
import threading
from typing import Dict, Optional, Any, Tuple, List

import yaml
from elastic_transport import HeadApiResponse, ObjectApiResponse
from elasticsearch import Elasticsearch, exceptions, logger
from es_config import MetricMapping

class ESHandler:
    """Elasticsearch 操作封装类，支持索引管理、数据CRUD及安全锁机制"""
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
        新增逻辑：索引不存在则先创建（用默认映射），再插入数据
        :param index_name: 索引名称
        :param doc_id: 文档ID（需唯一）
        :param data: 要写入的数据（JSON格式）
        :return: 成功返回True，失败返回False
        """
        with self.lock:  # 加锁保证原子性（索引创建+数据插入同锁内，避免并发问题）
            # 检查索引是否存在，不存在则用默认映射创建
            if not self.es.indices.exists(index=index_name):
                print(f"索引 '{index_name}' 不存在，自动创建（使用默认映射）")
                # 调用create_index，传入data_model的默认映射
                if not self.create_index(index_name, mappings=MetricMapping.DEFAULT_MAPPINGS):
                    print(f"索引 '{index_name}' 创建失败，无法添加数据")
                    return False  # 索引创建失败，直接返回

            # 检查文档ID是否存在
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

    def search(
            self,
            index_name: str,
            query: Dict,
            size: int = 10000,
            sort: Optional[List[Dict]] = None
    ) -> ObjectApiResponse[Any]:
        """
        执行批量查询（支持条件筛选）
        :param index_name: 索引名称
        :param query: 查询条件（ES 语法）
        :param size: 返回数量
        :param sort: 排序条件（可选，格式：[{"字段名": {"order": "desc/asc"}}]）
        :return: ES 原始响应
        """
        try:
            return self.es.search(
                index=index_name,
                query=query,
                size=size,
                sort=sort
            )
        except exceptions.RequestError as e:
            logger.error(f"批量查询失败：{e.error}（{e.info}）")
            raise  # 抛出异常由调用方处理

def init_es_handler(config_path: Optional[str] = None) -> Tuple[Optional[ESHandler], str]:
    """
    初始化 ESHandler 实例并返回索引名
    :param config_path: 配置文件路径（默认使用项目内 config/es_config.yaml）
    :return: (es_handler实例, 索引名) → 初始化失败时 es_handler 为 None
    """
    # 1. 确定配置文件路径
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
        es_handler = ESHandler(
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
    except Exception as e:
        print(f"ES 初始化异常：{str(e)}，ES 写入功能禁用")

    # 8. 初始化失败时，返回 None 和默认索引名
    return None, default_index
