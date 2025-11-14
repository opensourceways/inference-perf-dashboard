import unittest
from unittest.mock import Mock, patch
from ssl import SSLContext
from typing import Dict, Optional, Tuple
from es_command.es_operation import ESHandler, init_es_handler
from es_command.es_config import MetricMapping

class TestESHandler(unittest.TestCase):
    """ESHandler类的单元测试"""

    def setUp(self):
        """测试前置：初始化模拟ES连接"""
        self.mock_es = Mock()
        self.ssl_context = Mock(spec=SSLContext)
        
        # 替换ESHandler中的Elasticsearch实例为mock
        with patch('es_command.es_operation.Elasticsearch') as mock_es_cls:
            mock_es_cls.return_value = self.mock_es
            self.es_handler = ESHandler(
                es_url="https://mock.es:9200",
                username="test_user",
                token="test_token",
                ssl_context=self.ssl_context
            )

    def test_check_connection_success(self):
        """测试连接检查成功场景"""
        self.mock_es.info.return_value = {"status": "ok"}
        try:
            self.es_handler._check_connection()
        except Exception as e:
            self.fail(f"连接检查不应抛出异常，实际：{str(e)}")

    def test_check_connection_failure(self):
        """测试连接检查失败场景"""
        from elasticsearch.exceptions import ConnectionError as ESConnectionError
        self.mock_es.info.side_effect = ESConnectionError("连接失败")
        
        with self.assertRaises(ConnectionError):
            self.es_handler._check_connection()

    def test_create_index_new(self):
        """测试创建新索引"""
        self.mock_es.indices.exists.return_value = False
        self.mock_es.indices.create.return_value = {"acknowledged": True}
        
        result = self.es_handler.create_index("test_index")
        self.assertTrue(result)
        self.mock_es.indices.create.assert_called_once()

    def test_create_index_existing(self):
        """测试创建已存在的索引"""
        self.mock_es.indices.exists.return_value = True
        
        result = self.es_handler.create_index("test_index")
        self.assertFalse(result)
        self.mock_es.indices.create.assert_not_called()

    def test_add_data_success(self):
        """测试成功添加数据"""
        self.mock_es.indices.exists.return_value = True
        self.es_handler.check_id_exists = Mock(return_value=False)
        self.mock_es.index.return_value = {"result": "created"}
        
        result = self.es_handler.add_data("test_index", "doc_1", {"data": "test"})
        self.assertTrue(result)
        self.mock_es.index.assert_called_once()

    def test_add_data_existing_id(self):
        """测试添加已存在ID的数据"""
        self.mock_es.indices.exists.return_value = True
        self.es_handler.check_id_exists = Mock(return_value=True)
        
        result = self.es_handler.add_data("test_index", "doc_1", {"data": "test"})
        self.assertFalse(result)
        self.mock_es.index.assert_not_called()

    def test_update_data_success(self):
        """测试成功更新数据"""
        self.es_handler.check_id_exists = Mock(return_value=True)
        self.mock_es.update.return_value = {"result": "updated"}
        
        result = self.es_handler.update_data("test_index", "doc_1", {"field": "new_val"})
        self.assertTrue(result)

    def test_update_data_not_exists(self):
        """测试更新不存在的文档"""
        self.es_handler.check_id_exists = Mock(return_value=False)
        
        result = self.es_handler.update_data("test_index", "doc_1", {"field": "new_val"})
        self.assertFalse(result)

    def test_delete_data_success(self):
        """测试成功删除数据"""
        self.es_handler.check_id_exists = Mock(return_value=True)
        self.mock_es.delete.return_value = {"result": "deleted"}
        
        result = self.es_handler.delete_data("test_index", "doc_1")
        self.assertTrue(result)

    def test_get_data_success(self):
        """测试成功查询数据"""
        mock_response = {"_source": {"field": "value"}}
        self.mock_es.get.return_value = mock_response
        
        data = self.es_handler.get_data("test_index", "doc_1")
        self.assertEqual(data, {"field": "value"})

    def test_get_data_not_found(self):
        """测试查询不存在的文档"""
        from elasticsearch.exceptions import NotFoundError
        self.mock_es.get.side_effect = NotFoundError("未找到")
        
        data = self.es_handler.get_data("test_index", "doc_1")
        self.assertIsNone(data)

    def test_search_basic(self):
        """测试基础查询功能（无排序）"""
        mock_response = {"hits": {"hits": [{"_source": {"field": "value"}}]}}
        self.mock_es.search.return_value = mock_response
        
        query = {"term": {"ID": "test_id"}}
        result = self.es_handler.search("test_index", query, size=5)
        
        self.assertEqual(result, mock_response)
        self.mock_es.search.assert_called_once_with(
            index="test_index",
            body={"query": query, "size": 5}
        )

    def test_search_with_sort(self):
        """测试带排序条件的查询"""
        sort_config = [{"source.mean_e2e1_ms": {"order": "desc"}}]
        self.mock_es.search.return_value = {"hits": {"hits": []}}
        
        self.es_handler.search("test_index", {}, sort=sort_config)
        
        # 验证排序参数是否正确传递
        self.mock_es.search.assert_called_once_with(
            index="test_index",
            body={
                "query": {},
                "size": 10000,
                "sort": sort_config
            }
        )

    def test_search_request_error(self):
        """测试查询时发生RequestError异常"""
        from elasticsearch.exceptions import RequestError
        error_msg = "invalid query"
        self.mock_es.search.side_effect = RequestError(400, error_msg, {})
        
        with self.assertRaises(RequestError):
            self.es_handler.search("test_index", {"invalid": "query"})


class TestInitESHandler(unittest.TestCase):
    """init_es_handler函数的单元测试"""

    @patch('es_command.es_operation.os.path.exists')
    @patch('es_command.es_operation.open')
    @patch('es_command.es_operation.yaml.safe_load')
    @patch('es_command.es_operation.ESHandler')
    def test_init_success(self, mock_es_handler, mock_load, mock_open, mock_exists):
        """测试初始化成功场景"""
        mock_exists.return_value = True
        mock_load.return_value = {
            "es": {
                "url": "https://es:9200",
                "token": "test_token",
                "index_name": "test_index"
            }
        }
        
        handler, index = init_es_handler("fake_config.yaml")
        self.assertIsNotNone(handler)
        self.assertEqual(index, "test_index")
        mock_es_handler.assert_called_once()

    @patch('es_command.es_operation.os.path.exists')
    def test_init_config_not_found(self, mock_exists):
        """测试配置文件不存在场景"""
        mock_exists.return_value = False
        
        handler, index = init_es_handler("invalid_path.yaml")
        self.assertIsNone(handler)
        self.assertEqual(index, "sglang_model_performance")

    @patch('es_command.es_operation.os.path.exists')
    @patch('es_command.es_operation.open')
    @patch('es_command.es_operation.yaml.safe_load')
    def test_init_missing_config(self, mock_load, mock_open, mock_exists):
        """测试配置缺少必要字段场景"""
        mock_exists.return_value = True
        mock_load.return_value = {"es": {"url": "https://es:9200"}}  # 缺少token
        
        handler, index = init_es_handler("fake_config.yaml")
        self.assertIsNone(handler)


class TestMetricMapping(unittest.TestCase):
    """MetricMapping类的单元测试"""

    def test_default_mappings_structure(self):
        """测试默认映射结构正确性"""
        mappings = MetricMapping.DEFAULT_MAPPINGS
        self.assertIn("properties", mappings)
        self.assertIn("ID", mappings["properties"])
        self.assertIn("source", mappings["properties"])
        self.assertEqual(mappings["properties"]["ID"]["type"], "keyword")

    def test_update_default_mappings(self):
        """测试更新默认映射功能"""
        original = MetricMapping.DEFAULT_MAPPINGS.copy()
        new_mappings = {"properties": {"new_field": {"type": "text"}}}
        
        MetricMapping.update_default_mappings(new_mappings)
        self.assertEqual(MetricMapping.DEFAULT_MAPPINGS, new_mappings)
        
        # 恢复原始映射，避免影响其他测试
        MetricMapping.update_default_mappings(original)


if __name__ == '__main__':
    unittest.main()