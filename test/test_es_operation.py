# test/test_es_operation.py
import os
from unittest.mock import Mock, patch, MagicMock
import pytest
from elasticsearch import exceptions
from es_command.es_operation import ESHandler, init_es_handler
from es_command.es_config import MetricMapping


@pytest.fixture
def mock_es():
    """创建模拟的Elasticsearch客户端"""
    with patch('es_command.es_operation.Elasticsearch') as mock_es_cls:
        mock_es_instance = Mock()
        mock_es_cls.return_value = mock_es_instance
        yield mock_es_instance


@pytest.fixture
def es_handler(mock_es):
    """创建ESHandler实例（使用模拟ES客户端）"""
    ssl_context = Mock()
    handler = ESHandler(
        es_url="https://fake.es:9200",
        username="test_user",
        token="test_token",
        ssl_context=ssl_context
    )
    handler.es = mock_es  # 替换为模拟实例
    return handler


class TestESHandler:
    def test_check_connection_success(self, es_handler, mock_es):
        """测试连接成功场景"""
        mock_es.info.return_value = {"status": "ok"}
        es_handler._check_connection()  # 不应抛出异常

    def test_check_connection_failure(self, es_handler, mock_es):
        """测试连接失败场景"""
        mock_es.info.side_effect = exceptions.ConnectionError("连接失败")
        with pytest.raises(ConnectionError):
            es_handler._check_connection()

    def test_create_index_new(self, es_handler, mock_es):
        """测试创建新索引"""
        mock_es.indices.exists.return_value = False
        mock_es.indices.create.return_value = {"acknowledged": True}
        
        result = es_handler.create_index("test_index")
        assert result is True
        mock_es.indices.create.assert_called_once()

    def test_create_index_existing(self, es_handler, mock_es):
        """测试创建已存在的索引"""
        mock_es.indices.exists.return_value = True
        
        result = es_handler.create_index("test_index")
        assert result is False
        mock_es.indices.create.assert_not_called()

    def test_check_id_exists(self, es_handler, mock_es):
        """测试检查文档ID存在性"""
        mock_es.exists.return_value = True
        assert es_handler.check_id_exists("test_index", "doc123") is True
        
        mock_es.exists.return_value = False
        assert es_handler.check_id_exists("test_index", "doc123") is False

    def test_add_data_success(self, es_handler, mock_es):
        """测试成功添加数据"""
        mock_es.indices.exists.return_value = True
        mock_es.exists.return_value = False
        mock_es.index.return_value = {"result": "created"}
        
        data = {"ID": "doc123", "source": {"pr_id": "123"}}
        result = es_handler.add_data("test_index", "doc123", data)
        
        assert result is True
        mock_es.index.assert_called_once()

    def test_add_data_existing_id(self, es_handler, mock_es):
        """测试添加已存在ID的数据"""
        mock_es.indices.exists.return_value = True
        mock_es.exists.return_value = True
        
        data = {"ID": "doc123", "source": {"pr_id": "123"}}
        result = es_handler.add_data("test_index", "doc123", data)
        
        assert result is False
        mock_es.index.assert_not_called()

    def test_add_data_create_index_first(self, es_handler, mock_es):
        """测试添加数据时自动创建索引"""
        mock_es.indices.exists.side_effect = [False, True]  # 首次检查不存在，创建后存在
        mock_es.exists.return_value = False
        mock_es.index.return_value = {"result": "created"}
        es_handler.create_index = Mock(return_value=True)
        
        data = {"ID": "doc123", "source": {"pr_id": "123"}}
        result = es_handler.add_data("test_index", "doc123", data)
        
        assert result is True
        es_handler.create_index.assert_called_once_with("test_index", mappings=MetricMapping.DEFAULT_MAPPINGS)

    def test_update_data_success(self, es_handler, mock_es):
        """测试成功更新数据"""
        mock_es.exists.return_value = True
        mock_es.update.return_value = {"result": "updated"}
        
        result = es_handler.update_data("test_index", "doc123", {"source.pr_id": "456"})
        assert result is True

    def test_update_data_not_exists(self, es_handler, mock_es):
        """测试更新不存在的文档"""
        mock_es.exists.return_value = False
        
        result = es_handler.update_data("test_index", "doc123", {"source.pr_id": "456"})
        assert result is False

    def test_delete_data_success(self, es_handler, mock_es):
        """测试成功删除数据"""
        mock_es.exists.return_value = True
        mock_es.delete.return_value = {"result": "deleted"}
        
        result = es_handler.delete_data("test_index", "doc123")
        assert result is True

    def test_delete_data_not_exists(self, es_handler, mock_es):
        """测试删除不存在的文档"""
        mock_es.exists.return_value = False
        
        result = es_handler.delete_data("test_index", "doc123")
        assert result is False

    def test_get_data_success(self, es_handler, mock_es):
        """测试成功查询数据"""
        mock_es.get.return_value = {"_source": {"ID": "doc123", "source": {}}}
        
        data = es_handler.get_data("test_index", "doc123")
        assert data is not None
        assert data["ID"] == "doc123"

    def test_get_data_not_found(self, es_handler, mock_es):
        """测试查询不存在的文档"""
        mock_es.get.side_effect = exceptions.NotFoundError("未找到")
        
        data = es_handler.get_data("test_index", "doc123")
        assert data is None

    def test_search_success(self, es_handler, mock_es):
        """测试搜索成功并返回结果"""
        # 模拟索引存在
        mock_es.indices.exists.return_value = True
        # 模拟搜索结果
        mock_hits = [
            {"_source": {"ID": "doc1", "content": "test1"}},
            {"_source": {"ID": "doc2", "content": "test2"}}
        ]
        mock_es.search.return_value = {"hits": {"hits": mock_hits}}
        
        # 执行搜索
        query = {"match": {"content": "test"}}
        result = es_handler.search("test_index", query)
        
        # 验证结果
        assert result is not None
        assert len(result) == 2
        assert result[0]["ID"] == "doc1"
        assert result[1]["ID"] == "doc2"
        mock_es.search.assert_called_once_with(index="test_index", body={"query": query})


    def test_search_index_not_exists(self, es_handler, mock_es):
        """测试搜索不存在的索引"""
        mock_es.indices.exists.return_value = False
        
        query = {"match_all": {}}
        result = es_handler.search("non_existent_index", query)
        
        assert result == []
        mock_es.search.assert_not_called()


    def test_search_no_results(self, es_handler, mock_es):
        """测试搜索存在但无匹配结果"""
        mock_es.indices.exists.return_value = True
        mock_es.search.return_value = {"hits": {"hits": []}}  # 空结果
        
        query = {"match": {"content": "nonexistent"}}
        result = es_handler.search("test_index", query)
        
        assert result == []


    def test_search_with_exception(self, es_handler, mock_es):
        """测试搜索时发生异常"""
        mock_es.indices.exists.return_value = True
        mock_es.search.side_effect = exceptions.ConnectionError("搜索失败")
        
        query = {"match_all": {}}
        with pytest.raises(ConnectionError):
            es_handler.search("test_index", query)

class TestInitESHandler:
    @patch('es_command.es_operation.os.path.exists')
    @patch('es_command.es_operation.open')
    @patch('es_command.es_operation.yaml.safe_load')
    def test_init_success(self, mock_yaml, mock_open, mock_exists):
        """测试成功初始化ESHandler"""
        mock_exists.return_value = True
        mock_yaml.return_value = {
            "es": {
                "url": "https://fake.es:9200",
                "username": "admin",
                "token": "secret",
                "index_name": "test_index"
            }
        }
        
        with patch('es_command.es_operation.ESHandler') as mock_handler_cls:
            mock_handler = Mock()
            mock_handler_cls.return_value = mock_handler
            
            handler, index_name = init_es_handler()
            assert handler == mock_handler
            assert index_name == "test_index"

    @patch('es_command.es_operation.os.path.exists')
    def test_init_config_not_found(self, mock_exists):
        """测试配置文件不存在的情况"""
        mock_exists.return_value = False
        
        handler, index_name = init_es_handler(config_path="invalid.yaml")
        assert handler is None
        assert index_name == "sglang_model_performance"

    @patch('es_command.es_operation.os.path.exists')
    @patch('es_command.es_operation.open')
    @patch('es_command.es_operation.yaml.safe_load')
    def test_init_missing_config(self, mock_yaml, mock_open, mock_exists):
        """测试配置缺少必要字段的情况"""
        mock_exists.return_value = True
        mock_yaml.return_value = {"es": {"url": "https://fake.es:9200"}}  # 缺少token
        
        handler, index_name = init_es_handler()
        assert handler is None
        assert index_name == "sglang_model_performance"