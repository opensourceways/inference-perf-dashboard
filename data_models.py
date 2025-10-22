from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class Metric:
    """模型性能指标类，包含延迟、吞吐量等核心指标"""
    avg_e2el: float  # 平均端到端延迟（ms）
    avg_ttft: float  # 平均首token生成时间（ms）
    avg_tpot: float  # 平均每个输出token时间（ms）
    avg_itl: float  # 平均token间隔延迟（ms）
    p99_e2el: float  # P99分位端到端延迟（ms）
    p99_ttft: float  # P99分位首token时间（ms）
    p99_tpot: float  # P99分位每个输出token时间（ms）
    p99_itl: float  # P99分位token间隔延迟（ms）
    max_concurrency: int  # 最大并发数
    request_throughput: float  # 请求吞吐量（req/s）
    total_input_tokens: int  # 总输入tokens
    total_generated_tokens: int  # 总生成tokens
    input_token_throughput: float  # 输入token吞吐量（token/s）
    output_token_throughput: float  # 输出token吞吐量（token/s）
    total_token_throughput: float  # 总token吞吐量（token/s）


@dataclass
class PRInfo:
    """PR信息类，包含PR编号、日期、作者等元信息"""
    pr_id: str
    pr_date: str  # 格式：YYYY-MM-DD
    pr_time: str = None  # 格式：HH:MM:SS（可选）
    pr_author: str = None  # 提交者（可选）
    pr_author_email: str = None  # 提交者邮箱（可选）
    pr_body: str = None  # PR描述（可选）


def create_metric_from_test_data(test_data: Dict[str, Any]) -> Metric:
    """
    从测试数据字典创建Metric对象（包含字段校验）
    参数:
        test_data: 包含所有Metric字段的字典
    异常:
        ValueError: 若缺少字段或类型错误
    """
    # 校验必填字段
    required_fields = Metric.__annotations__.keys()
    missing_fields = [f for f in required_fields if f not in test_data]
    if missing_fields:
        raise ValueError(f"Metric数据缺少必填字段: {missing_fields}")

    # 类型转换与实例化
    try:
        return Metric(
            avg_e2el=float(test_data["avg_e2el"]),
            avg_ttft=float(test_data["avg_ttft"]),
            avg_tpot=float(test_data["avg_tpot"]),
            avg_itl=float(test_data["avg_itl"]),
            p99_e2el=float(test_data["p99_e2el"]),
            p99_ttft=float(test_data["p99_ttft"]),
            p99_tpot=float(test_data["p99_tpot"]),
            p99_itl=float(test_data["p99_itl"]),
            max_concurrency=int(test_data["max_concurrency"]),
            request_throughput=float(test_data["request_throughput"]),
            total_input_tokens=int(test_data["total_input_tokens"]),
            total_generated_tokens=int(test_data["total_generated_tokens"]),
            input_token_throughput=float(test_data["input_token_throughput"]),
            output_token_throughput=float(test_data["output_token_throughput"]),
            total_token_throughput=float(test_data["total_token_throughput"])
        )
    except (TypeError, ValueError) as e:
        raise type(e)(f"Metric字段类型错误: {e}")
