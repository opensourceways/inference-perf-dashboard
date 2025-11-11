import json
import os
import sys
from unittest.mock import Mock, patch

import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app, es_api_handler, adjust_model_params, format_commit_log, format_data_details_compares_log, \
    format_data_details_log


class TestApp:
    """app.py 单元测试类"""

    @pytest.fixture
    def client(self):
        """创建测试客户端"""
        app.config['TESTING'] = True
        with app.test_client() as client:
            yield client

    @pytest.fixture
    def mock_es_handler(self):
        """模拟 ESHandler"""
        mock_handler = Mock()
        mock_handler.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "source": {
                                "model_name": "test-model",
                                "sglang_branch": "main",
                                "device": "A100",
                                "commit_id": "abc123",
                                "merged_at": "2024-01-01T10:00:00"
                            }
                        }
                    }
                ]
            }
        }
        return mock_handler

    @pytest.fixture
    def valid_params(self):
        """有效的查询参数"""
        return {
            "startTime": 1700000000,
            "endTime": 1700086400,
            "models": "model1,model2",
            "engineVersion": 0,
            "size": 1000
        }

    def test_adjust_model_params(self):
        """测试参数调整函数"""
        # 测试正常模型列表
        params = {"models": ["model1", "model2"]}
        result = adjust_model_params(params)
        assert result["model_names"] == ["model1", "model2"]

        # 测试 "all" 特殊情况
        params_all = {"models": ["all"]}
        result_all = adjust_model_params(params_all)
        assert result_all["model_names"] is None

    def test_format_commit_log(self):
        """测试提交列表日志格式化"""
        params = {}
        result = {"model1": [{"hash": "abc", "time": 123}], "model2": [{"hash": "def", "time": 456}]}
        log_msg = format_commit_log(params, result)
        assert "模型数=2" in log_msg
        assert "总记录数=2" in log_msg

    def test_format_data_details_compares_log(self):
        """测试数据对比日志格式化"""
        params = {
            "model_names": ["model1"],
            "engineVersion": 1,
            "startTime": 1700000000,
            "endTime": 1700086400
        }
        result = [{"name": "model1", "device": "A100"}]
        log_msg = format_data_details_compares_log(params, result)
        assert "模型详情查询完成" in log_msg
        assert "返回数据条数=1" in log_msg

    def test_format_data_details_log(self):
        """测试数据详情日志格式化"""
        params = {
            "model_names": ["model1", "model2"],
            "engineVersion": 2
        }
        result = [{"model_name": "model1"}, {"model_name": "model2"}]
        log_msg = format_data_details_log(params, result)
        assert "模型列表查询完成" in log_msg
        assert "返回模型数=2" in log_msg

    def test_health_check_success(self, client, mock_es_handler):
        """测试健康检查接口 - 成功情况"""
        with patch('app.es_handler', mock_es_handler):
            mock_es_handler.es.ping.return_value = True

            response = client.get('/health')
            data = json.loads(response.data)

            assert response.status_code == 200
            assert data["status"] == "healthy"
            assert data["es_status"] == "connected"

    def test_health_check_es_disconnected(self, client):
        """测试健康检查接口 - ES断开情况"""
        with patch('app.es_handler', None):
            response = client.get('/health')
            data = json.loads(response.data)

            assert response.status_code == 200
            assert data["status"] == "healthy"
            assert data["es_status"] == "disconnected"

    def test_es_api_handler_es_not_ready(self, client):
        """测试ES API处理器 - ES未就绪"""
        with patch('app.es_handler', None):
            response = client.get('/server/commits/list')
            data = json.loads(response.data)

            assert response.status_code == 500
            assert not data["success"]
            assert "ES连接未就绪" in data["message"]

    def test_es_api_handler_missing_required_params(self, client, mock_es_handler):
        """测试ES API处理器 - 缺少必填参数"""
        with patch('app.es_handler', mock_es_handler):
            response = client.get('/server/commits/list')
            data = json.loads(response.data)

            assert response.status_code == 400
            assert not data["success"]
            assert "缺失必填参数" in data["message"]

    def test_es_api_handler_invalid_engine_version(self, client, mock_es_handler):
        """测试ES API处理器 - 无效的engineVersion"""
        with patch('app.es_handler', mock_es_handler):
            params = {
                "startTime": 1700000000,
                "endTime": 1700086400,
                "models": "model1",
                "engineVersion": 5  # 无效值
            }
            response = client.get('/server/commits/list', query_string=params)
            data = json.loads(response.data)

            assert response.status_code == 400
            assert not data["success"]
            assert "engineVersion无效" in data["message"]

    def test_es_api_handler_invalid_time_range(self, client, mock_es_handler):
        """测试ES API处理器 - 无效时间范围"""
        with patch('app.es_handler', mock_es_handler):
            params = {
                "startTime": 1700086400,  # 开始时间大于结束时间
                "endTime": 1700000000,
                "models": "model1",
                "engineVersion": 0
            }
            response = client.get('/server/commits/list', query_string=params)
            data = json.loads(response.data)

            assert response.status_code == 400
            assert not data["success"]
            assert "时间范围无效" in data["message"]

    def test_es_api_handler_empty_models(self, client, mock_es_handler):
        """测试ES API处理器 - 空模型列表"""
        with patch('app.es_handler', mock_es_handler):
            params = {
                "startTime": 1700000000,
                "endTime": 1700086400,
                "models": ",,",  # 只有分隔符
                "engineVersion": 0
            }
            response = client.get('/server/commits/list', query_string=params)
            data = json.loads(response.data)

            assert response.status_code == 400
            assert not data["success"]
            assert "models参数不可为空" in data["message"]

    def test_es_api_handler_es_connection_error(self, client, mock_es_handler):
        """测试ES API处理器 - ES连接错误"""
        with patch('app.es_handler', mock_es_handler):
            from elasticsearch import exceptions
            mock_es_handler.search.side_effect = exceptions.ConnectionError("Connection failed")

            params = {
                "startTime": 1700000000,
                "endTime": 1700086400,
                "models": "model1",
                "engineVersion": 0
            }
            response = client.get('/server/commits/list', query_string=params)
            data = json.loads(response.data)

            assert response.status_code == 500
            assert not data["success"]
            assert "ES连接失败" in data["message"]

    def test_es_api_handler_general_exception(self, client, mock_es_handler):
        """测试ES API处理器 - 通用异常"""
        with patch('app.es_handler', mock_es_handler):
            mock_es_handler.search.side_effect = Exception("Unexpected error")

            params = {
                "startTime": 1700000000,
                "endTime": 1700086400,
                "models": "model1",
                "engineVersion": 0
            }
            response = client.get('/server/commits/list', query_string=params)
            data = json.loads(response.data)

            assert response.status_code == 500
            assert not data["success"]
            assert "服务内部错误" in data["message"]

    def test_get_server_commits_list_success(self, client, mock_es_handler):
        """测试提交列表接口 - 成功情况"""
        with patch('app.es_handler', mock_es_handler), \
                patch('app.es_index_name', 'test_index'):
            params = {
                "startTime": 1700000000,
                "endTime": 1700086400,
                "models": "model1,model2",
                "engineVersion": 0
            }
            response = client.get('/server/commits/list', query_string=params)
            json.loads(response.data)

            assert response.status_code == 200
            # 验证ES查询被调用
            mock_es_handler.search.assert_called_once()

    def test_get_server_data_details_compare_list_success(self, client, mock_es_handler):
        """测试数据对比接口 - 成功情况"""
        with patch('app.es_handler', mock_es_handler), \
                patch('app.es_index_name', 'test_index'):
            # 为数据对比接口准备特定的响应数据
            mock_compare_response = {
                "hits": {
                    "hits": [
                        {
                            "_source": {
                                "source": {
                                    "model_name": "model1",
                                    "tp": 1,
                                    "request_rate": 10,
                                    "device": "A100",
                                    "mean_e2el_ms": 100.0,
                                    "mean_itl_ms": 50.0,
                                    "mean_tpot_ms": 30.0,
                                    "mean_ttft_ms": 20.0,
                                    "p99_itl_ms": 100.0,
                                    "p99_tpot_ms": 60.0,
                                    "p99_ttft_ms": 40.0,
                                    "request_throughput": 5.0,
                                    "output_token_throughput": 100.0,
                                    "total_token_throughput": 150.0,
                                    "commit_id": "commit1",
                                    "merged_at": "2024-01-01T10:00:00"
                                }
                            }
                        }
                    ]
                }
            }
            mock_es_handler.search.return_value = mock_compare_response

            params = {
                "startTime": 1700000000,
                "endTime": 1700086400,
                "models": "model1",
                "engineVersion": 1
            }
            response = client.get('/server/data-details-compare/list', query_string=params)

            assert response.status_code == 200
            mock_es_handler.search.assert_called_once()

    def test_get_server_data_details_list_success(self, client, mock_es_handler):
        """测试数据详情接口 - 成功情况"""
        with patch('app.es_handler', mock_es_handler), \
                patch('app.es_index_name', 'test_index'):
            params = {
                "startTime": 1700000000,
                "endTime": 1700086400,
                "models": "model1",
                "engineVersion": 2
            }
            response = client.get('/server/data-details/list', query_string=params)

            assert response.status_code == 200
            mock_es_handler.search.assert_called_once()

    def test_es_api_handler_integration(self, mock_es_handler):
        """测试ES API处理器的集成流程"""
        with patch('app.es_handler', mock_es_handler), \
                patch('app.es_index_name', 'test_index'):
            # 模拟调整参数函数
            adjust_params = Mock(return_value={
                "model_names": ["test-model"],
                "engineVersion": 0,
                "startTime": 1700000000,
                "endTime": 1700086400,
                "size": 10000
            })
            process_response = Mock(return_value={"success": True, "data": []})

            format_log = Mock(return_value="Test log message")

            with app.test_request_context(
                    '/test?startTime=1700000000&endTime=1700086400&models=test-model&engineVersion=0'):
                # 调用es_api_handler
                api_func = es_api_handler(adjust_params, process_response, format_log)
                response = api_func()

                adjust_params.assert_called_once()
                process_response.assert_called_once()
                format_log.assert_called_once()
                mock_es_handler.search.assert_called_once()

                assert response.status_code == 200

    def test_all_models_query(self, client, mock_es_handler):
        """测试查询所有模型的情况"""
        with patch('app.es_handler', mock_es_handler), \
                patch('app.es_index_name', 'test_index'):
            params = {
                "startTime": 1700000000,
                "endTime": 1700086400,
                "models": "all",
                "engineVersion": 0
            }
            response = client.get('/server/commits/list', query_string=params)

            assert response.status_code == 200
            # 验证adjust_model_params正确处理了"all"参数
            mock_es_handler.search.assert_called_once()


# 运行测试的配置
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
