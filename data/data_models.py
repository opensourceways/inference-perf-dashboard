from dataclasses import dataclass
from typing import Dict


@dataclass
class Metric:
    """模型性能指标类，包含延迟、吞吐量等核心指标"""
    __dataclass_fields__ = None
    model_name: str
    device: str
    status: str
    request_rate: float
    mean_e2el_ms: float  # 平均端到端延迟（ms）
    mean_ttft_ms: float  # 平均首token生成时间（ms）
    mean_tpot_ms: float  # 平均每个输出token时间（ms）
    mean_itl_ms: float  # 平均token间隔延迟（ms）
    p99_e2el_ms: float  # P99分位端到端延迟（ms）
    p99_ttft_ms: float  # P99分位首token时间（ms）
    p99_tpot_ms: float  # P99分位每个输出token时间（ms）
    p99_itl_ms: float  # P99分位token间隔延迟（ms）
    median_e2el_ms: float
    median_ttft_ms: float
    median_tpot_ms: float
    median_itl_ms: float
    max_concurrency: int  # 最大并发数
    request_throughput: float  # 请求吞吐量（req/s）
    total_input_tokens: int  # 总输入tokens
    total_generated_tokens: int  # 总生成tokens
    input_token_throughput: float  # 输入token吞吐量（token/s）
    output_token_throughput: float  # 输出token吞吐量（token/s）
    total_token_throughput: float  # 总token吞吐量（token/s）


@dataclass
class PRInfo:
    """PR信息类，包含PR编号、日期、分支等元信息"""
    pr_id: str
    commit_id: str
    commit_title: str
    created_at: str
    sglang_branch: str = None

