import pandas as pd
import json
import os
from datetime import datetime
from typing import Dict, Any, List, Tuple
from dataclasses import asdict
from data_models import Metric, PRInfo, create_metric_from_test_data


ROOT_DIR = os.path.expanduser("~/.cache/aisbench")

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


def create_metrics_data(
        csv_path: str,
        metrics_json_path: str,
        pr_json_path: str,
        model_name: str,
        stage: str = "stable"
) -> Dict[str, Dict]:
    """
    从文件生成metrics_data结构
    参数:
        csv_path: 性能指标CSV路径
        metrics_json_path: 统计信息JSON路径
        pr_json_path: PR信息JSON路径
        model_name: 模型名称（如"Qwen3-32B"）
        stage: 阶段（如"stable"）
    返回:
        按commit_id组织的metrics_data字典
    """
    # 解析基础数据
    pr_info, commit_id = parse_pr_json(pr_json_path)  # PR信息 + commit_id
    csv_metrics = parse_csv_metrics(csv_path, stage)  # CSV指标
    json_metrics = parse_metrics_json(metrics_json_path, stage)  # JSON指标

    # 生成复合ID：格式 "commit_id_model_name"（确保唯一标识）
    composite_id = f"{commit_id}_{model_name}"

    # 整合source内容：PR信息（字典格式） + 合并后的指标（字典格式）
    pr_info_dict = asdict(pr_info)  # 将PRInfo对象转为字典
    full_metrics_dict = merge_metrics(csv_metrics, json_metrics)  # 合并指标
    source = {**pr_info_dict, **full_metrics_dict}  # 合并PR信息和指标

    # 返回目标格式
    return {
        "ID": composite_id,
        "source": source
    }


def batch_create_metrics_data(model_configs: List[Dict[str, str]]) -> List[Dict[str, Dict]]:
    """
    批量生成目标格式数据：返回列表，每个元素是单模型的 {"ID": ..., "source": ...}
        参数:
        model_configs: 模型配置列表，每个配置包含csv_path、metrics_json_path、pr_json_path、model_name
    返回:
        合并后的metrics_data
    """
    metrics_data_list = []
    for config in model_configs:
        try:
            # 生成单模型目标格式数据
            single_model_data = create_metrics_data(
                csv_path=config["csv_path"],
                metrics_json_path=config["metrics_json_path"],
                pr_json_path=config["pr_json_path"],
                model_name=config["model_name"],
                stage=config.get("stage", "stable")
            )
            metrics_data_list.append(single_model_data)
        except Exception as e:
            print(f"处理模型 {config['model_name']} 失败: {str(e)}")
            continue

    return metrics_data_list

def get_subdir_names(dir_path: str) -> List[str]:
    """获取子目录名称"""
    subdir_names = []

    for entry in os.listdir(dir_path):
        entry_path = os.path.join(dir_path, entry)
        if os.path.isdir(entry_path):
            subdir_names.append(entry)

    return subdir_names

def get_data_dir(date_str: str = None) -> tuple[str, str, str]:
    """
    根据传入的日期字符串生成数据目录路径，默认使用当前日期

    参数:
        date_str: 可选，指定日期（格式：YYYYMMDD，如"20251022"）
                  若为None，则自动使用当前日期

    返回:
        tuple: (current_date_str, date_dir, commit_dir)
              - current_date_str: 日期字符串（YYYYMMDD）
              - date_dir: 日期目录完整路径（ROOT_DIR/YYYYMMDD）
              - commit_dir: commit子目录完整路径（ROOT_DIR/YYYYMMDD/commit_id）
    """
    # 1. 确定日期字符串：优先使用传入的date_str，否则用当前日期
    if date_str:
        # 校验传入的日期格式是否正确（YYYYMMDD）
        try:
            # 尝试解析为日期对象，验证格式有效性
            datetime.strptime(date_str, "%Y%m%d")
            current_date_str = date_str
        except ValueError:
            raise ValueError(f"传入的date_str格式错误，应为YYYYMMDD，实际为：{date_str}")
    else:
        # 无传入日期时，使用当前日期（YYYYMMDD）
        current_date = datetime.now().date()
        current_date_str = current_date.strftime("%Y%m%d")

    # 2. 生成目录路径
    date_dir = os.path.join(ROOT_DIR, current_date_str)
    commit_dir = os.path.join(date_dir, "commit_id")

    return current_date_str, date_dir, commit_dir


def generate_metrics_data() -> List[Dict[str, Dict]]:
    """
    核心逻辑：获取日期/Commit目录、遍历模型子目录、判断文件存在性、生成metrics数据
    返回：所有有效模型的metrics数据列表（跳过文件不存在的模型）
    """
    # 获取动态路径参数
    try:
        current_date_str, date_dir, commit_dir_full = get_data_dir('20251022')  # 需返回 (当前日期字符串, 日期目录名)
        model_names = get_subdir_names(commit_dir_full)  # 需返回 commit 目录下的模型子目录名列表（如 ["Qwen3-32B", ...]）
    except Exception as e:
        print(f"获取动态路径参数失败：{str(e)}")
        return []

    # 存储所有有效模型的metrics数据
    all_valid_metrics = []

    # 遍历每个模型，生成配置并判断文件存在性
    for model_name in model_names:
        # 构建当前模型的3个关键文件路径
        csv_path = os.path.join(ROOT_DIR, current_date_str, "commit_id", model_name, "gsm8kdataset.csv")
        metrics_json_path = os.path.join(ROOT_DIR, current_date_str, "commit_id", model_name, "gsm8kdataset.json")
        pr_json_path = os.path.join(ROOT_DIR, current_date_str, "commit_id", "pr.json")

        # 判断文件是否存在（3个文件需同时存在，否则跳过）
        missing_files = []
        if not os.path.exists(csv_path):
            missing_files.append(f"CSV文件: {csv_path}")
        if not os.path.exists(metrics_json_path):
            missing_files.append(f"指标JSON文件: {metrics_json_path}")
        if not os.path.exists(pr_json_path):
            missing_files.append(f"PR JSON文件: {pr_json_path}")

        if missing_files:
            print(f"模型 {model_name} 跳过：缺少以下文件 → {', '.join(missing_files)}")
            continue  # 文件不全，跳过当前模型

        # 构建当前模型的配置（文件存在时才处理）
        model_config = [
            {
                "model_name": model_name,
                "csv_path": csv_path,
                "metrics_json_path": metrics_json_path,
                "pr_json_path": pr_json_path,
                "stage": "stable"
            }
        ]

        # 生成当前模型的metrics数据
        try:
            model_metrics = batch_create_metrics_data(model_config)
            if not model_metrics:
                print(f"模型 {model_name} 无有效数据，跳过")
                continue
            current_data = model_metrics[0]  # 单个模型仅1条数据
            all_valid_metrics.append(current_data)
        except Exception as e:
            print(f"模型 {model_name} 数据生成失败：{str(e)}")
            continue

        # 定义输出目录和文件名（确保目录存在，避免写入失败）
        output_root_dir = "output"
        os.makedirs(output_root_dir, exist_ok=True)

        # 生成文件名：格式“20251022_commit_id_Qwen3-32B.json”（commit_id 用实际目录名，如需真实哈希可调整）
        output_filename = f"{current_date_str}_commit_id_{model_name}.json"
        output_file = os.path.join(output_root_dir, output_filename)

        # 去重逻辑：分步骤判断文件存在性 + ID一致性，细化异常处理
        id_exists = False  # 标记ID是否已存在
        if os.path.exists(output_file):
            print(f"检测到模型 {model_name} 已有文件：{output_filename}，开始校验ID...")
            try:
                # 读取已有文件（限制读取大小，避免超大文件占用资源）
                with open(output_file, "r", encoding="utf-8") as f:
                    # 读取文件内容（若文件过大，可拆分为按行读取，此处简化用json.load）
                    existing_data = json.load(f)

                # 提取已有数据的ID
                existing_id = None
                if isinstance(existing_data, list):
                    # 格式1：列表（如 [{"ID": "...", ...}]）→ 取第一个有效元素的ID
                    if not existing_data:  # 空列表，无有效ID
                        print(f"模型 {model_name} 已有文件为空列表，无有效ID")
                    else:
                        first_item = existing_data[0]
                        if "ID" not in first_item:
                            raise KeyError("文件列表中的数据缺少必填字段 'ID'")
                        existing_id = first_item["ID"]
                elif isinstance(existing_data, dict):
                    # 格式2：字典（如 {"ID": "...", "source": ...}）→ 直接取ID
                    if "ID" not in existing_data:
                        raise KeyError("文件字典数据缺少必填字段 'ID'")
                    existing_id = existing_data["ID"]
                else:
                    # 不支持的格式（如字符串、数字）
                    raise TypeError(f"文件数据格式不支持（仅支持列表/字典），实际格式：{type(existing_data).__name__}")

                # 对比当前数据ID与已有ID（校验当前数据是否有ID字段）
                if "ID" not in current_data:
                    raise KeyError(f"当前模型 {model_name} 生成的数据缺少必填字段 'ID'")
                current_id = current_data["ID"]

                # 判断ID是否重复
                if existing_id == current_id:
                    id_exists = True
                    print(f"模型 {model_name} 跳过：文件已包含相同ID（{current_id}），无需重复写入")
                else:
                    print(f"模型 {model_name} 已有文件ID（{existing_id}）与当前ID（{current_id}）不一致，将覆盖文件")

            # 异常处理
            except json.JSONDecodeError:
                print(f"模型 {model_name} 已有文件格式错误（非标准JSON），将覆盖文件")
            except KeyError as e:
                print(f"模型 {model_name} 已有文件数据异常：{str(e)}，将覆盖文件")
            except TypeError as e:
                print(f"模型 {model_name} 已有文件数据格式异常：{str(e)}，将覆盖文件")
            except Exception as e:
                # 其他未预见的错误（如权限不足、文件损坏）
                print(f"检查模型 {model_name} 已有文件时发生未知错误：{str(e)}，将覆盖文件")
        else:
            # 文件不存在，无需校验ID
            print(f"模型 {model_name} 无已有文件，准备写入新文件")

        # 无重复ID时，写入文件
        if not id_exists:
            try:
                # 写入当前模型的单独文件（无需合并，每个模型一个文件）
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump([current_data], f, indent=2, ensure_ascii=False)  # 用列表包裹，保持格式统一
                print(f"模型 {model_name} 数据已保存：{os.path.abspath(output_file)}")
            except Exception as e:
                print(f"保存模型 {model_name} 数据失败：{str(e)}")

    return all_valid_metrics


if __name__ == "__main__":
    print("开始生成metrics数据...")
    final_data = generate_metrics_data()

    # 打印最终结果摘要
    if final_data:
        print(f"\n生成完成！共处理 {len(final_data)} 个有效模型")
        print("最终数据示例（第一个模型）：")
        print(json.dumps(final_data[0], indent=2, ensure_ascii=False))
    else:
        print("\n生成完成，但未获取到任何有效模型数据")

