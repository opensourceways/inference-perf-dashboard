from collections import defaultdict

import pandas as pd
import json
import os
from datetime import datetime
from typing import Dict, Any, List, Tuple, DefaultDict, Optional
from dataclasses import asdict

import yaml
from fastapi import exceptions

from data_models import Metric, PRInfo
from ..es_command.es_operation import ESHandler

ROOT_DIR = os.path.expanduser("~/.cache/aisbench")

def parse_metrics_csv(csv_path: str, stage: str = "stable") -> Dict[str, float | int]:
    """解析性能CSV，返回Metric类所需字段（匹配类定义，含延迟/总token数）"""
    # 读取CSV并按stage过滤
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"CSV文件不存在: {csv_path}")

    df_stage = df[df["Stage"] == stage].copy()
    if df_stage.empty:
        raise ValueError(f"CSV中未找到stage='{stage}'的数据")

    # 定义CSV参数与Metric字段的映射 格式：{CSV参数: {Metric字段名: 取值列名, 数据类型}}
    param_mapping = {
        # 延迟类参数：E2EL/TTFT/TPOT/ITL（对应mean/median/p99）
        "E2EL": {
            "mean_e2el_ms": ("Average", float),
            "median_e2el_ms": ("Median", float),
            "p99_e2el_ms": ("P99", float)
        },
        "TTFT": {
            "mean_ttft_ms": ("Average", float),
            "median_ttft_ms": ("Median", float),
            "p99_ttft_ms": ("P99", float)
        },
        "TPOT": {
            "mean_tpot_ms": ("Average", float),
            "median_tpot_ms": ("Median", float),
            "p99_tpot_ms": ("P99", float)
        },
        "ITL": {
            "mean_itl_ms": ("Average", float),
            "median_itl_ms": ("Median", float),
            "p99_itl_ms": ("P99", float)
        },
        # 总token数：InputTokens→总输入，OutputTokens→总生成
        "InputTokens": {
            "total_input_tokens": ("Average", float)
        },
        "OutputTokens": {
            "total_generated_tokens": ("Average", float)
        }
    }

    # 获取Metric类的有效字段（避免生成类中不存在的字段）
    metric_fields = {f.name for f in Metric.__dataclass_fields__.values()}
    parsed_data: Dict[str, float | int] = {}

    # 按映射解析CSV数据
    for _, row in df_stage.iterrows():
        param = row["Performance Parameters"]
        if param not in param_mapping:
            continue  # 跳过CSV中无需解析的参数（如N列相关）

        # 处理当前参数的所有Metric字段映射
        for metric_field, (csv_col, data_type) in param_mapping[param].items():
            if metric_field not in metric_fields:
                continue  # 跳过Metric类中不存在的字段

            # 清理数值（去除"ms"单位，转成目标类型）
            raw_value = str(row[csv_col]).replace(" ms", "").strip()
            parsed_data[metric_field] = data_type(raw_value)

    required_from_csv = [
        # 延迟类必需字段
        "mean_e2el_ms", "mean_ttft_ms", "mean_tpot_ms", "mean_itl_ms", "median_e2el_ms", "median_ttft_ms",
        "median_tpot_ms", "median_itl_ms", "p99_e2el_ms", "p99_ttft_ms", "p99_tpot_ms", "p99_itl_ms",
        "total_input_tokens", "total_generated_tokens"
    ]
    # 过滤出Metric类中存在但未解析到的字段
    missing_fields = [f for f in required_from_csv if f in metric_fields and f not in parsed_data]
    if missing_fields:
        raise ValueError(f"CSV解析缺失Metric必需字段：{missing_fields}（文件：{csv_path}）")

    return parsed_data

def parse_metrics_json(json_path: str, stage: str = "stable") -> Dict[str, Any]:
    """解析JSON，返回 Metric 类所需的“并发/吞吐量”字段（按类字段类型自动转换）"""
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"指标JSON文件不存在: {json_path}")
    except json.JSONDecodeError:
        raise ValueError(f"指标JSON格式错误: {json_path}")

    # Metric 类字段名与JSON键的映射
    json_to_metric_map = {
        "Max Concurrency": "max_concurrency",
        "Request Throughput": "request_throughput",
        "Total Input Tokens": "total_input_tokens",
        "Total generated tokens": "total_generated_tokens",
        "Input Token Throughput": "input_token_throughput",
        "Output Token Throughput": "output_token_throughput",
        "Total Token Throughput": "total_token_throughput"
    }
    metric_field_names = {field.name for field in Metric.__dataclass_fields__.values()}
    json_metrics = {}

    for json_key, metric_key in json_to_metric_map.items():
        # 跳过 Metric 类中不存在的字段
        if metric_key not in metric_field_names:
            continue
        # 获取JSON原始值并处理单位
        raw_value = json_data[json_key][stage]
        if isinstance(raw_value, str):
            # 移除单位（req/s 或 token/s）
            cleaned_value = raw_value.replace(" req/s", "").replace(" token/s", "")
        else:
            cleaned_value = raw_value

        # 按 Metric 类字段的类型转换值（确保类型匹配，如int/float）
        metric_field_type = Metric.__dataclass_fields__[metric_key].type
        try:
            json_metrics[metric_key] = metric_field_type(cleaned_value)
        except (ValueError, TypeError):
            raise ValueError(
                f"JSON字段 {json_key} 的值 {raw_value} 无法转换为 Metric.{metric_key} 的类型 {metric_field_type.__name__}"
            )

    # 校验：确保JSON解析出所有“仅在JSON中获取”的 Metric 必需字段
    json_required_fields = [
        "max_concurrency", "request_throughput", "total_input_tokens", "total_generated_tokens",
        "input_token_throughput", "output_token_throughput", "total_token_throughput"
    ]
    missing_fields = [f for f in json_required_fields if f in metric_field_names and f not in json_metrics]
    if missing_fields:
        raise ValueError(f"JSON解析缺失 Metric 必需字段：{missing_fields}（文件：{json_path}）")

    return json_metrics

def parse_pr_json(pr_json_path: str) -> Tuple[PRInfo, str]:
    """解析PR JSON，返回 PRInfo 对象和 commit_id"""
    try:
        with open(pr_json_path, "r", encoding="utf-8") as f:
            pr_data = json.load(f)
            # 确保JSON是字典格式（避免数组等非预期格式）
            if not isinstance(pr_data, dict):
                raise ValueError(f"PR JSON格式错误：应为字典，实际为{type(pr_data).__name__}")
    except FileNotFoundError:
        raise FileNotFoundError(f"PR JSON文件不存在: {pr_json_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"PR JSON格式错误（解析失败）: {pr_json_path}，详情：{str(e)}")

    required_fields = {"pr_id", "commit_id", "commit_title", "created_at", "sglang_branch"}
    # 获取JSON中实际存在的字段
    actual_fields = set(pr_data.keys())
    # 检查缺失的必填字段
    missing_fields = required_fields - actual_fields
    if missing_fields:
        raise ValueError(f"PR JSON缺少必填字段: {sorted(missing_fields)}（需包含{required_fields}）")

    empty_fields = [field for field in required_fields if not str(pr_data[field]).strip()]
    if empty_fields:
        raise ValueError(f"PR JSON字段值为空: {empty_fields}（需填写有效内容）")

    created_at = pr_data["created_at"].strip()
    try:
        # 仅校验格式，不转换（确保输入即符合目标格式）
        datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        raise ValueError(
            f"created_at格式错误: {created_at}（必须为YYYY-MM-DDTHH:MM:SS，示例：2025-10-22T14:51:00）"
        )

    pr_info = PRInfo(
        pr_id=pr_data["pr_id"].strip(),
        commit_id=pr_data["commit_id"].strip(),
        commit_title=pr_data["commit_title"].strip(),
        created_at=created_at,
        sglang_branch=pr_data["sglang_branch"].strip()
    )

    return pr_info, pr_data["commit_id"].strip()

def merge_metrics(csv_metrics: Dict[str, float], json_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """合并CSV和JSON指标，先生成 Metric 对象（确保字段完整），再转为字典"""
    # 合并所有指标字段
    all_metric_fields = {**csv_metrics, **json_metrics}
    # 校验：确保覆盖 Metric 类的所有字段（核心！避免扩展字段漏解析）
    metric_required_fields = [field.name for field in Metric.__dataclass_fields__.values()]
    missing_fields = [f for f in metric_required_fields if f not in all_metric_fields]
    if missing_fields:
        raise ValueError(f"合并指标缺失 Metric 必需字段：{missing_fields}")

    # 生成 Metric 对象（强类型校验，确保数据合法）
    metric_obj = Metric(**all_metric_fields)
    # 转为字典（用于后续与 PR 信息整合）
    return asdict(metric_obj)

def create_metrics_data(
        csv_path: str,
        metrics_json_path: str,
        pr_json_path: str,
        model_name: str,
        stage: str = "stable"
) -> Dict[str, Dict]:
    """生成目标格式数据：整合 PRInfo + Metric（基于 Metric 类确保指标完整）"""
    # 解析基础数据（PR信息 + 分源指标）
    pr_info, commit_id = parse_pr_json(pr_json_path)
    csv_metrics = parse_metrics_csv(csv_path, stage)
    json_metrics = parse_metrics_json(metrics_json_path, stage)

    json_metrics["model_name"] = model_name
    json_metrics["device"] = "Ascend910B3"

    # 合并指标（生成 Metric 对象后转为字典，确保字段完整）
    full_metrics_dict = merge_metrics(csv_metrics, json_metrics)

    # 生成复合ID（保留原逻辑：commit_id + model_name，确保唯一）
    composite_id = f"{commit_id}_{model_name}"

    # 整合 PR 信息与指标（source 包含 PR 字段 + 完整 Metric 字段）
    source = {
        **asdict(pr_info),  # PRInfo 转为字典
        **full_metrics_dict  # 完整 Metric 字段
    }

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

def get_date_str(date_str: str = None) -> str:
    """
    根据传入的日期字符串生成数据目录路径，默认使用当前日期

    参数:
        date_str: 可选，指定日期（格式：YYYYMMDD，如"20251022"）
                  若为None，则自动使用当前日期

    返回:
        tuple: (current_date_str: 日期字符串（YYYYMMDD）
    """
    # 确定日期字符串：优先使用传入的date_str，否则用当前日期
    if date_str:
        # 校验传入的日期格式是否正确（YYYYMMDD）
        try:
            # 尝试解析为日期对象，验证格式有效性
            datetime.strptime(date_str, "%Y%m%d")
            current_date_str = date_str
            return current_date_str
        except ValueError:
            raise ValueError(f"传入的date_str格式错误，应为YYYYMMDD，实际为：{date_str}")
    else:
        # 无传入日期时，使用当前日期（YYYYMMDD）
        current_date = datetime.now().date()
        current_date_str = current_date.strftime("%Y%m%d")
        return current_date_str

def get_dynamic_paths(date_str: str = None, commit_id: str = None) -> tuple[str, str, str, list[str]]:
    """
    根据传入的日期字符串生成数据目录路径，默认使用当前日期
    """
    current_date_str = get_date_str(date_str)

    # 生成目录路径
    date_dir = os.path.join(ROOT_DIR, current_date_str)
    commit_dir_full = os.path.join(date_dir, commit_id)

    try:
        # 获取commit目录下的模型子目录名
        model_names = get_subdir_names(commit_dir_full)
        if not model_names:
            print(f"警告：commit目录 {commit_dir_full} 下无模型子目录")

        return current_date_str, date_dir, commit_dir_full, model_names
    except Exception as e:
        raise Exception(f"获取动态路径失败：{str(e)}")

def check_model_files(current_date_str: str, commit_id: str, model_name: str) -> Tuple[bool, List[str], Dict[str, str]]:
    """
    校验当前模型的CSV、指标JSON、PR JSON文件是否存在
    参数:
        current_date_str: 日期字符串（YYYYMMDD）
        model_name: 模型名称
    返回:
        (is_valid, missing_files, file_paths)
        - is_valid: 是否所有文件齐全
        - missing_files: 缺失的文件列表
        - file_paths: 所有文件的完整路径（文件齐全时有效）
    """
    # 构建3个关键文件的路径
    file_paths = {
        "csv_path": os.path.join(ROOT_DIR, current_date_str, commit_id, model_name, "gsm8kdataset.csv"),
        "metrics_json_path": os.path.join(ROOT_DIR, current_date_str, commit_id, model_name, "gsm8kdataset.json"),
        "pr_json_path": os.path.join(ROOT_DIR, current_date_str, commit_id, "pr.json")
    }

    # 检查文件存在性
    missing_files = []
    for file_type, file_path in file_paths.items():
        if not os.path.exists(file_path):
            missing_files.append(f"{file_type.replace('_path', '')}：{file_path}")

    return len(missing_files) == 0, missing_files, file_paths

def generate_single_model_data(model_name: str, file_paths: Dict[str, str]) -> Dict[str, Any]:
    """
    根据文件路径生成当前模型的metrics数据
    参数:
        model_name: 模型名称
        file_paths: 文件路径字典（csv_path、metrics_json_path、pr_json_path）
    返回:
        单个模型的metrics数据（含ID和source）
    """
    try:
        # 构建模型配置
        model_config = [
            {
                "model_name": model_name,
                "csv_path": file_paths["csv_path"],
                "metrics_json_path": file_paths["metrics_json_path"],
                "pr_json_path": file_paths["pr_json_path"],
                "stage": "stable"
            }
        ]
        # 生成数据
        model_metrics = batch_create_metrics_data(model_config)
        if not model_metrics:
            raise Exception("无有效数据生成")
        return model_metrics[0]  # 单个模型仅1条数据
    except Exception as e:
        raise Exception(f"数据生成失败：{str(e)}")

def write_model_data_to_file(current_date_str: str, commit_id: str, model_name: str, current_data: Dict[str, Any]) -> None:
    """
    处理模型数据的写入，含去重逻辑（ID存在则跳过，否则写入）
    参数:
        current_date_str: 日期字符串（YYYYMMDD）
        model_name: 模型名称
        current_data: 单个模型的metrics数据
    """
    # 准备输出路径和文件名
    output_root_dir = "output"
    os.makedirs(output_root_dir, exist_ok=True)  # 确保目录存在
    output_filename = f"{current_date_str}_{commit_id}_{model_name}.json"
    output_file = os.path.join(output_root_dir, output_filename)

    # 去重判断
    id_exists = False
    if os.path.exists(output_file):
        print(f"检测到模型 {model_name} 已有文件：{output_filename}，校验ID...")
        id_exists = _check_existing_id(output_file, current_data)  # 内部辅助函数

    # 写入文件（无重复ID时）
    if not id_exists:
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump([current_data], f, indent=2, ensure_ascii=False)
            print(f"模型 {model_name} 数据已保存：{os.path.abspath(output_file)}")
        except Exception as e:
            print(f"保存模型 {model_name} 数据失败：{str(e)}")
    else:
        print(f"模型 {model_name} 跳过：已有相同ID数据")

def write_aggregated_files(
    total_data: List[Dict[str, Any]],
    commit_id_grouped: Dict[str, List[Dict[str, Any]]],
    date_grouped: Dict[str, List[Dict[str, Any]]],
    current_date_str: str,
    commit_id: str  # 当前批次的commit_id（从PR文件中提取，确保唯一）
) -> None:
    """
    生成三类聚合文件：总表、commit_id维度、日期维度
    参数:
        total_data: 总表数据（所有模型数据列表）
        commit_id_grouped: 按commit_id分组的数据（key: commit_id，value: 该commit下所有模型数据）
        date_grouped: 按日期分组的数据（key: 日期字符串，value: 该日期下所有模型数据）
        current_date_str: 当前处理的日期（YYYYMMDD）
        commit_id: 当前批次的commit_id（从PR文件提取，确保分组key唯一）
    """
    output_root_dir = "output"
    os.makedirs(output_root_dir, exist_ok=True)  # 确保输出目录存在

    # 生成【总表文件】（所有模型数据汇总）
    total_file = os.path.join(output_root_dir, "metrics_total.json")
    try:
        with open(total_file, "w", encoding="utf-8") as f:
            json.dump(total_data, f, indent=2, ensure_ascii=False)
        print(f"总表文件已保存：{os.path.abspath(total_file)}（共{len(total_data)}条数据）")
    except Exception as e:
        print(f"保存总表文件失败：{str(e)}")

    # 生成【commit_id维度文件】（按commit_id分组，单个commit一个文件）
    for cid, cid_data in commit_id_grouped.items():
        commit_file = os.path.join(output_root_dir, f"metrics_commit_{cid}.json")
        try:
            with open(commit_file, "w", encoding="utf-8") as f:
                json.dump(cid_data, f, indent=2, ensure_ascii=False)
            print(f"commit_id维度文件已保存：{os.path.abspath(commit_file)}（共{len(cid_data)}条数据）")
        except Exception as e:
            print(f"保存commit_id={cid}文件失败：{str(e)}")

    # 生成【日期维度文件】（按日期分组，单个日期一个文件）
    for date_str, date_data in date_grouped.items():
        date_file = os.path.join(output_root_dir, f"metrics_date_{date_str}.json")
        try:
            with open(date_file, "w", encoding="utf-8") as f:
                json.dump(date_data, f, indent=2, ensure_ascii=False)
            print(f"日期维度文件已保存：{os.path.abspath(date_file)}（共{len(date_data)}条数据）")
        except Exception as e:
            print(f"保存日期={date_str}文件失败：{str(e)}")

def _check_existing_id(output_file: str, current_data: Dict[str, Any]) -> bool:
    """内部辅助：检查已有文件的ID是否与当前数据ID重复"""
    try:
        # 读取已有文件
        with open(output_file, "r", encoding="utf-8") as f:
            existing_data = json.load(f)

        # 提取已有ID
        existing_id = _extract_id_from_data(existing_data, "已有文件")
        # 提取当前数据ID
        current_id = _extract_id_from_data(current_data, "当前数据")

        # 对比ID
        if existing_id == current_id:
            return True
        else:
            print(f"模型ID不匹配（已有：{existing_id}，当前：{current_id}），将覆盖文件")
            return False

    except json.JSONDecodeError:
        print(f"已有文件格式错误（非标准JSON），将覆盖文件")
        return False
    except Exception as e:
        print(f"校验ID时出错：{str(e)}，将覆盖文件")
        return False

def _extract_id_from_data(data: Any, data_type: str) -> str:
    """内部辅助：从数据（列表/字典）中提取ID，无ID则抛异常"""
    if isinstance(data, list):
        if not data:
            raise Exception(f"{data_type}为空列表，无有效ID")
        first_item = data[0]
        if "ID" not in first_item:
            raise Exception(f"{data_type}列表中的数据缺少'ID'字段")
        return first_item["ID"]
    elif isinstance(data, dict):
        if "ID" not in data:
            raise Exception(f"{data_type}字典缺少'ID'字段")
        return data["ID"]
    else:
        raise Exception(f"{data_type}格式不支持（仅列表/字典），实际：{type(data).__name__}")

def ensure_unique_id(
        target_list: List[Dict[str, Any]],
        new_item: Dict[str, Any],
        existing_ids: set  # 传入外部维护的ID集合，避免重复遍历列表
) -> bool:
    """
    检查新数据的ID是否已存在，确保唯一性
    参数:
        target_list: 要添加数据的目标列表（如total_data、cid_data等）
        new_item: 待添加的新数据
        existing_ids: 已存在的ID集合（用于快速校验，避免O(n)遍历）
    返回:
        bool: 新数据是否成功添加（True=添加，False=重复跳过）
    """
    # 提取新数据的ID（确保ID字段存在）
    new_id = new_item.get("ID")
    if not new_id:
        raise ValueError("待添加的数据缺少'ID'字段，无法进行唯一性校验")

    # 检查ID是否已存在
    if new_id in existing_ids:
        print(f"发现重复ID：{new_id}，已跳过该数据")
        return False
    else:
        # 新增数据，更新列表和ID集合
        target_list.append(new_item)
        existing_ids.add(new_id)
        return True

def init_es_handler(config_path: Optional[str] = None) -> Tuple[Optional[ESHandler], str]:
    """
    初始化 ESHandler 实例并返回索引名
    :param config_path: 配置文件路径（默认使用项目内 config/config.yaml）
    :return: (es_handler实例, 索引名) → 初始化失败时 es_handler 为 None
    """
    # 1. 确定配置文件路径（默认路径：当前文件目录下的 config/config.yaml）
    if not config_path:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_dir, "config", "es_config.yaml")

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
    except (ConnectionError, exceptions.ConnectionError):
        print(f"ES 连接失败：无法连接到 {es_url}，ES 写入功能禁用")
    except (PermissionError, exceptions.AuthenticationException):
        print(f"ES 认证失败：用户名或密码错误，ES 写入功能禁用")
    except Exception as e:
        print(f"ES 初始化异常：{str(e)}，ES 写入功能禁用")

    # 8. 初始化失败时，返回 None 和默认索引名
    return None, default_index

def generate_metrics_data(target_date: str = "20251022") -> List[Dict[str, Any]]:
    """
    主函数：确保生成 模型-commit_id组合数据 + 单commit_id数据 + 单日期数据 + 总表数据
    返回：所有有效模型的metrics数据列表（确保多维度数据完整）
    """
    # 初始化聚合容器（多维度数据存储，确保不丢失）
    # - 总表：所有有效数据汇总
    total_data: List[Dict[str, Any]] = []
    total_existing_ids: set = set()

    # - commit_id维度：key=commit_id，value=该commit下所有模型数据
    commit_id_grouped: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    commit_existing_ids: DefaultDict[str, set] = defaultdict(set)

    # - 日期维度：key=日期字符串（YYYYMMDD），value=该日期下所有模型数据
    date_grouped: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    date_existing_ids: DefaultDict[str, set] = defaultdict(set)

    # - 模型-commit_id组合记录：确保每个组合都有数据（用于后续校验）
    model_commit_pairs: set = set()

    all_valid_metrics: List[Dict[str, Any]] = []
    print(f"=== 开始生成metrics数据（目标日期：{target_date}）===")

    try:
        # 获取基础路径（日期目录、所有commit_id列表）
        current_date_str = get_date_str(target_date)  # 如"20251022"
        date_dir_full = os.path.join(ROOT_DIR, current_date_str)
        commit_ids = get_subdir_names(date_dir_full)  # 所有commit_id目录列表

        if not commit_ids:
            print(f"日期目录 {date_dir_full} 下无commit_id子目录，无法生成多维度数据")
            return all_valid_metrics

        # 遍历每个commit_id（核心：不提前返回，确保所有commit都处理）
        for commit_id in commit_ids:
            print(f"===== 开始处理 commit_id：{commit_id} =====")
            try:
                # 获取当前commit下的模型列表（调用get_dynamic_paths，需返回4个参数：日期str、日期目录、commit目录、模型列表）
                _, _, commit_dir_full, model_names = get_dynamic_paths(target_date, commit_id)

                if not model_names:
                    print(f"commit_id {commit_id} 下无模型子目录，跳过该commit")
                    # 即使无模型，也需在commit_id维度保留空列表（确保该commit有对应记录）
                    commit_id_grouped[commit_id] = []
                    continue

                # 遍历当前commit下的每个模型（处理模型-commit_id组合）
                for model_name in model_names:
                    print(f"--- 处理 模型-commit组合：{model_name}@{commit_id} ---")
                    # 记录组合（用于后续校验）
                    pair_key = f"{model_name}@{commit_id}"
                    if pair_key in model_commit_pairs:
                        print(f"模型-commit组合 {pair_key} 已处理，跳过重复数据")
                        continue

                    # 校验模型所需文件（确保文件齐全）
                    is_file_valid, missing_files, file_paths = check_model_files(
                        current_date_str, commit_id, model_name
                    )
                    if not is_file_valid:
                        print(f"组合 {pair_key} 跳过：缺少文件 → {', '.join(missing_files)}")
                        continue

                    es_handler, es_index_name = init_es_handler()
                    # 生成单个模型数据
                    try:
                        current_data = generate_single_model_data(model_name, file_paths)
                        if not current_data or "ID" not in current_data:
                            raise ValueError("生成的模型数据为空或缺少必填字段'ID'")

                        # 将性能数据写入到ES数据库
                        if es_handler:  # 仅当 ESHandler 初始化成功时执行
                            print(f"开始将组合 {pair_key} 写入 ES（ID：{current_data['ID']}）")
                            # 调用 ESHandler.add_data：索引名 + doc_id（用current_data["ID"]） + 数据
                            es_write_success = es_handler.add_data(
                                index_name=es_index_name,
                                doc_id=current_data["ID"],  # 用数据自带的ID作为ES文档ID，避免重复
                                data=current_data
                            )
                            if es_write_success:
                                print(f"组合 {pair_key} 写入 ES 成功")
                            else:
                                print(f"组合 {pair_key} 写入 ES 失败（数据已生成但未同步到ES）")
                        else:
                            print(f"ESHandler 未初始化，组合 {pair_key} 未写入 ES")

                        # 写入【模型-commit_id组合文件】（单文件，确保组合数据存在）
                        write_model_data_to_file(current_date_str, commit_id, model_name, current_data)
                        print(f"组合 {pair_key} 单文件写入成功")

                        # 收集多维度聚合数据（去重后加入）
                        # - 总表去重
                        if ensure_unique_id(total_data, current_data, total_existing_ids):
                            print(f"组合 {pair_key} 加入总表（去重后总表共{len(total_data)}条）")
                            # - commit_id维度去重（当前commit）
                            if ensure_unique_id(
                                commit_id_grouped[commit_id], current_data, commit_existing_ids[commit_id]
                            ):
                                print(f"组合 {pair_key} 加入commit_id={commit_id}分组（共{len(commit_id_grouped[commit_id])}条）")
                                # - 日期维度去重（当前日期）
                                if ensure_unique_id(
                                    date_grouped[current_date_str], current_data, date_existing_ids[current_date_str]
                                ):
                                    print(f"组合 {pair_key} 加入日期={current_date_str}分组（共{len(date_grouped[current_date_str])}条）")

                        # 标记组合已处理
                        model_commit_pairs.add(pair_key)
                        all_valid_metrics.append(current_data)
                        print(f"组合 {pair_key} 全维度数据处理完成")

                    except Exception as e:
                        print(f"组合 {pair_key} 数据生成失败：{str(e)}")
                        continue

            except Exception as e:
                print(f"commit_id {commit_id} 处理异常：{str(e)}，继续处理下一个commit")
                continue

        # 所有commit和模型处理完成后，生成【多维度聚合文件】（确保一次生成，避免重复）
        print(f"===== 所有commit处理完成，开始生成聚合文件 =====")
        # 确保日期维度有当前日期数据（即使为空，也保留空列表）
        if current_date_str not in date_grouped:
            date_grouped[current_date_str] = []
        # 生成总表、commit_id维度、日期维度文件
        write_aggregated_files(
            total_data=total_data,
            commit_id_grouped=commit_id_grouped,
            date_grouped=date_grouped,
            current_date_str=current_date_str,
            commit_id=commit_ids[-1]  # 传入最后一个commit_id（仅用于函数参数兼容，不影响分组）
        )

        # 数据完整性校验（打印汇总信息，确保多维度数据存在）
        print(f"===== 数据完整性校验结果 =====")
        print(f"1. 模型-commit组合数：{len(model_commit_pairs)}（已处理的唯一组合）")
        print(f"2. commit_id维度数：{len(commit_id_grouped)}（每个commit都有对应数据）")
        print(f"3. 日期维度数：{len(date_grouped)}（当前日期{current_date_str}已包含）")
        print(f"4. 总表数据量：{len(total_data)}（去重后所有有效数据）")

    except Exception as e:
        print(f"全局处理异常：{str(e)}，但已尽力保留已处理数据")

    # 最终返回所有有效模型数据（确保非空）
    print(f"=== 整体处理完成！共生成 {len(all_valid_metrics)} 个有效模型数据 ===")
    return all_valid_metrics if all_valid_metrics else []



# ---------------------- 函数调用（主入口） ----------------------
if __name__ == "__main__":
    # 可指定目标日期，如 generate_metrics_data("20251023")
    generate_metrics_data(target_date="20251022")

