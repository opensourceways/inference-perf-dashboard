from dataclasses import dataclass


@dataclass
class Metric:
    """模型性能指标类，包含延迟、吞吐量等核心指标"""
    __dataclass_fields__ = None
    model_name: str
    device: str
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
    commit_id: str
    pr_date: str  # 格式：YYYY-MM-DD
    pr_time: str = None  # 格式：HH:MM:SS（可选）
    pr_branch: str = None
    pr_author: str = None  # 提交者（可选）
    pr_author_email: str = None  # 提交者邮箱（可选）
    pr_body: str = None  # PR描述（可选）

