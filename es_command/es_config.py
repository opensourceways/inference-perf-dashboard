from typing import Dict


class MetricMapping:
    """模型性能数据的ES映射管理类"""
    # 默认映射（适配模型性能数据结构）
    DEFAULT_MAPPINGS = {
        "properties": {
            "ID": {"type": "keyword"},
            "source": {
                "properties": {
                    "pr_id": {"type": "keyword"},
                    "status": {"type": "keyword"},
                    "tp": {"type": "integer"},
                    "engine_version": {"type": "keyword"},
                    "commit_id": {"type": "keyword"},
                    "commit_title": {"type": "text"},
                    "created_at": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss"},
                    "sglang_branch": {"type": "keyword"},
                    "model_name": {"type": "keyword"},
                    "device": {"type": "keyword"},
                    "request_rate": {"type": "float"},
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

    @classmethod
    def update_default_mappings(cls, new_mappings: Dict) -> None:
        """更新默认映射（影响所有引用该类的地方）"""
        cls.DEFAULT_MAPPINGS = new_mappings
        print("默认映射已更新")