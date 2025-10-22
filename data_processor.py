import pandas as pd
import json
from datetime import datetime
from typing import Dict, Any, List, Tuple
from dataclasses import asdict
from data_models import Metric, PRInfo, create_metric_from_test_data


def parse_csv_metrics(csv_path: str, stage: str = "stable") -> Dict[str, float]:
    """解析性能指标CSV文件，提取指定阶段（如stable）的指标"""
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"CSV文件不存在: {csv_path}")

    df_stage = df[df["Stage"] == stage].copy()
    if df_stage.empty:
        raise ValueError(f"CSV中未找到stage={stage}的数据")

    csv_metrics = {}
    for _, row in df_stage.iterrows():
        param = row["Performance Parameters"]
        # 解析延迟类指标（E2EL/TTFT/TPOT/ITL）
        if param in ["E2EL", "TTFT", "TPOT", "ITL"]:
            avg_key = f"avg_{param.lower()}"
            p99_key = f"p99_{param.lower()}"
            # 移除单位（如" ms"）并转换为float
            csv_metrics[avg_key] = float(row["Average"].replace(" ms", ""))
            csv_metrics[p99_key] = float(row["P99"].replace(" ms", ""))
        # 解析输出token吞吐量
        elif param == "OutputTokenThroughput":
            csv_metrics["output_token_throughput"] = float(row["Average"].replace(" token/s", ""))

    return csv_metrics


def parse_metrics_json(json_path: str, stage: str = "stable") -> Dict[str, Any]:
    """解析统计信息JSON文件（如吞吐量、并发数）"""
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"指标JSON文件不存在: {json_path}")
    except json.JSONDecodeError:
        raise ValueError(f"指标JSON格式错误: {json_path}")

    # 提取核心指标
    return {
        "max_concurrency": int(json_data["Max Concurrency"][stage]),
        "request_throughput": float(json_data["Request Throughput"][stage].replace(" req/s", "")),
        "total_input_tokens": int(json_data["Total Input Tokens"][stage]),
        "total_generated_tokens": int(json_data["Total generated tokens"][stage]),
        "input_token_throughput": float(json_data["Input Token Throughput"][stage].replace(" token/s", "")),
        "total_token_throughput": float(json_data["Total Token Throughput"][stage].replace(" token/s", ""))
    }


def parse_pr_json(pr_json_path: str) -> Tuple[PRInfo, str]:
    """解析PR信息JSON文件，返回PRInfo对象和对应的commit_id"""
    try:
        with open(pr_json_path, "r", encoding="utf-8") as f:
            pr_data = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"PR JSON文件不存在: {pr_json_path}")
    except json.JSONDecodeError:
        raise ValueError(f"PR JSON格式错误: {pr_json_path}")

    # 校验PR必填字段
    required_fields = ["pr_id", "commit_id", "pr_date"]
    missing_fields = [f for f in required_fields if f not in pr_data]
    if missing_fields:
        raise ValueError(f"PR JSON缺少必填字段: {missing_fields}")

    # 处理日期格式（20251022 → 2025-10-22）
    try:
        pr_date = datetime.strptime(pr_data["pr_date"], "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        raise ValueError(f"pr_date格式错误（应为YYYYMMDD）: {pr_data['pr_date']}")

    # 处理时间格式（20251022144611 → 14:46:11）
    pr_time = None
    if "pr_time" in pr_data and pr_data["pr_time"]:
        try:
            time_str = pr_data["pr_time"][-6:]  # 截取后6位（HHMMSS）
            pr_time = datetime.strptime(time_str, "%H%M%S").strftime("%H:%M:%S")
        except ValueError:
            raise ValueError(f"pr_time格式错误（应为YYYYMMDDHHMMSS）: {pr_data['pr_time']}")

    # 构建PRInfo对象
    pr_info = PRInfo(
        pr_id=pr_data["pr_id"],
        pr_date=pr_date,
        pr_time=pr_time,
        pr_author=pr_data.get("pr_subcommiter"),  # pr_subcommiter对应作者
        pr_author_email=None,  # 文件中无该字段
        pr_body=None  # 文件中无该字段
    )

    return pr_info, pr_data["commit_id"]


def merge_metrics(csv_metrics: Dict[str, float], json_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """合并CSV和JSON解析的指标，生成完整的Metric数据"""
    return {**csv_metrics, **json_metrics}


def create_test_data(
        csv_path: str,
        metrics_json_path: str,
        pr_json_path: str,
        model_name: str,
        stage: str = "stable"
) -> Dict[str, Dict]:
    """
    从文件生成test_data结构
    参数:
        csv_path: 性能指标CSV路径
        metrics_json_path: 统计信息JSON路径
        pr_json_path: PR信息JSON路径
        model_name: 模型名称（如"Qwen3-32B"）
        stage: 阶段（如"stable"）
    返回:
        按commit_id组织的test_data字典
    """
    # 解析PR信息和commit_id
    pr_info, commit_id = parse_pr_json(pr_json_path)

    # 解析并合并性能指标
    csv_metrics = parse_csv_metrics(csv_path, stage)
    json_metrics = parse_metrics_json(metrics_json_path, stage)
    full_metrics = merge_metrics(csv_metrics, json_metrics)

    # 创建Metric对象
    metric = create_metric_from_test_data(full_metrics)

    # 构建test_data
    return {
        commit_id: {
            "pr_info": pr_info,
            "metric_info": {model_name: metric}
        }
    }


def batch_create_test_data(model_configs: List[Dict[str, str]]) -> Dict[str, Dict]:
    """
    批量处理多个模型，生成合并后的test_data
    参数:
        model_configs: 模型配置列表，每个配置包含csv_path、metrics_json_path、pr_json_path、model_name
    返回:
        合并后的test_data（按commit_id分组）
    """
    test_data = {}
    for config in model_configs:
        try:
            # 生成单个模型的test_data
            model_data = create_test_data(
                csv_path=config["csv_path"],
                metrics_json_path=config["metrics_json_path"],
                pr_json_path=config["pr_json_path"],
                model_name=config["model_name"],
                stage=config.get("stage", "stable")
            )
        except Exception as e:
            print(f"处理模型 {config['model_name']} 失败: {str(e)}")
            continue

        # 合并到总test_data（同一commit_id下的模型合并）
        commit_id = next(iter(model_data.keys()))
        if commit_id not in test_data:
            test_data[commit_id] = model_data[commit_id]
        else:
            test_data[commit_id]["metric_info"].update(model_data[commit_id]["metric_info"])

    return test_data


if __name__ == "__main__":
    # 示例：批量处理模型配置
    sample_configs = [
        {
            "model_name": "Qwen3-32B",
            "csv_path": "qwen3_32b_metrics.csv",  # 替换为实际路径
            "metrics_json_path": "qwen3_32b_stats.json",  # 替换为实际路径
            "pr_json_path": "qwen3_32b_pr.json",  # 替换为实际路径
            "stage": "stable"
        },
        {
            "model_name": "DeepSeek-V3",
            "csv_path": "deepseek_v3_metrics.csv",
            "metrics_json_path": "deepseek_v3_stats.json",
            "pr_json_path": "deepseek_v3_pr.json",
            "stage": "stable"
        }
    ]

    # 生成test_data
    test_data = batch_create_test_data(sample_configs)

    # 转换为字典格式（便于序列化/存储）
    test_data_dict = {
        cid: {
            "pr_info": asdict(data["pr_info"]),
            "metric_info": {m: asdict(met) for m, met in data["metric_info"].items()}
        }
        for cid, data in test_data.items()
    }

    # 打印结果（实际场景可写入文件或数据库）
    import pprint

    print("生成的test_data:")
    pprint.pprint(test_data_dict)
