import argparse
import json
import os
from dataclasses import asdict, fields
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Set

import pandas as pd

from config import logger_config
from data.data_models import Metric, PRInfo
from es_command import es_operation

logger = logger_config.get_logger(__name__)

ROOT_DIR = os.path.expanduser("/data/ascend-ci-share-pkking-sglang/aisbench")
METRIC_CSV_DIR = "gsm8kdataset.csv"
METRIC_JSON_DIR = "gsm8kdataset.json"
PR_INFO_DIR = 'pr.json'

def parse_metrics_csv(csv_path: str, stage: str = "total") -> Dict[str, float | int]:
    """解析性能CSV，返回Metric类所需字段"""
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
    metric_fields = {field.name for field in fields(Metric)}
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


def parse_metrics_json(json_path: str, stage: str = "total") -> Dict[str, Any]:
    """解析JSON，返回 Metric 类所需的“并发/吞吐量”字段"""
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
        "Total Token Throughput": "total_token_throughput",
        "tp": "tp",
        "request_rate": "request_rate"
    }
    metric_field_names = {field.name for field in fields(Metric)}
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
        fields_type_mapping = {field.name: field.type for field in fields(Metric)}
        metric_field_type = fields_type_mapping[metric_key]
        try:
            json_metrics[metric_key] = metric_field_type(cleaned_value)
        except (ValueError, TypeError):
            raise ValueError(
                f"JSON字段 {json_key} 的值 {raw_value} 无法转换为 Metric.{metric_key} 的类型 {metric_field_type.__name__}"
            )

    # 校验：确保JSON解析出所有“仅在JSON中获取”的 Metric 必需字段
    json_required_fields = [
        "max_concurrency", "request_throughput", "total_input_tokens", "total_generated_tokens",
        "input_token_throughput", "output_token_throughput", "total_token_throughput", "tp", "request_rate"
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
            if not isinstance(pr_data, dict):
                raise ValueError(f"PR JSON格式错误：应为字典，实际为{type(pr_data).__name__}")
    except FileNotFoundError:
        raise FileNotFoundError(f"PR JSON文件不存在: {pr_json_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"PR JSON格式错误（解析失败）: {pr_json_path}，详情：{str(e)}")

    required_fields = {"pr_id", "commit_id", "pr_title", "merged_at", "sglang_branch", "device"}
    actual_fields = set(pr_data.keys())
    missing_fields = required_fields - actual_fields
    if missing_fields:
        raise ValueError(f"PR JSON缺少必填字段: {sorted(missing_fields)}（需包含{required_fields}）")

    empty_fields = [field for field in required_fields if not str(pr_data[field]).strip()]
    if empty_fields:
        raise ValueError(f"PR JSON字段值为空: {empty_fields}（需填写有效内容）")

    merged_at = pr_data["merged_at"].strip()
    try:
        datetime.strptime(merged_at, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        raise ValueError(
            f"merged_at格式错误: {merged_at}（必须为YYYY-MM-DDTHH:MM:SS，示例：2025-10-22T14:51:00）"
        )

    pr_info = PRInfo(
        pr_id=pr_data["pr_id"].strip(),
        commit_id=pr_data["commit_id"].strip(),
        pr_title=pr_data["pr_title"].strip(),
        merged_at=merged_at,
        sglang_branch=pr_data["sglang_branch"].strip(),
        device=pr_data["device"].strip()
    )

    return pr_info, pr_data["commit_id"].strip()


def merge_metrics(csv_metrics: Dict[str, float], json_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """合并CSV和JSON指标，先生成 Metric 对象（确保字段完整），再转为字典"""
    # 合并所有指标字段
    all_metric_fields = {**csv_metrics, **json_metrics}
    # 校验：确保覆盖 Metric 类的所有字段
    metric_required_fields = [field.name for field in fields(Metric)]
    missing_fields = [f for f in metric_required_fields if f not in all_metric_fields]
    if missing_fields:
        raise ValueError(f"合并指标缺失 Metric 必需字段：{missing_fields}")

    # 生成 Metric 对象
    metric_obj = Metric(**all_metric_fields)
    return asdict(metric_obj)


def create_metrics_data(
        csv_path: str,
        metrics_json_path: str,
        pr_json_path: str,
        model_name: str,
        stage: str = "total"
) -> Dict[str, Dict]:
    """生成目标格式数据：整合 PRInfo + Metric（基于 Metric 类确保指标完整）"""
    # 解析基础数据
    pr_info, commit_id = parse_pr_json(pr_json_path)
    csv_metrics = parse_metrics_csv(csv_path, stage)
    json_metrics = parse_metrics_json(metrics_json_path, stage)

    json_metrics["model_name"] = model_name
    json_metrics["status"] = "normal"
    json_metrics["engine_version"] = '0'
    request_rate = int(json_metrics["request_rate"])

    # 合并指标
    full_metrics_dict = merge_metrics(csv_metrics, json_metrics)

    # 生成复合ID
    composite_id = f"{commit_id}_{model_name}_{request_rate}"

    # 整合 PR 信息与指标
    source = {
        **asdict(pr_info),  # PRInfo 转为字典
        **full_metrics_dict  # 完整 Metric 字段
    }

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
                stage=config.get("stage", "total")
            )
            metrics_data_list.append(single_model_data)
        except Exception as e:
            logger.error(f"处理模型 {config['model_name']} 失败: {str(e)}")
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
    根据传入的日期字符串生成数据目录路径，默认使用T+1日期
    参数:
        date_str: 可选，指定日期（格式：YYYYMMDD，如"20251022"）若为None，则自动使用T+1日期
    返回:
        tuple: (current_date_str: 日期字符串（YYYYMMDD）
    """
    if date_str:
        # 校验传入的日期格式是否正确（YYYYMMDD）
        try:
            datetime.strptime(date_str, "%Y%m%d")
            current_date_str = date_str
            return current_date_str
        except ValueError:
            raise ValueError(f"传入的date_str格式错误，应为YYYYMMDD，实际为：{date_str}")
    else:
        yesterday = datetime.now().date() - timedelta(days=1)
        current_date_str = yesterday.strftime("%Y%m%d")
        return current_date_str


def check_model_files(current_date_str, commit_id, model_name, request_rate):
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
        "csv_path": os.path.join(ROOT_DIR, current_date_str, commit_id, model_name, request_rate, METRIC_CSV_DIR),
        "metrics_json_path": os.path.join(ROOT_DIR, current_date_str, commit_id, model_name, request_rate, METRIC_JSON_DIR),
        "pr_json_path": os.path.join(ROOT_DIR, current_date_str, commit_id, PR_INFO_DIR)
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
                "stage": "total"
            }
        ]
        # 生成数据
        model_metrics = batch_create_metrics_data(model_config)
        if not model_metrics:
            raise Exception("无有效数据生成")
        return model_metrics[0]
    except Exception as e:
        raise Exception(f"数据生成失败：{str(e)}")


def _check_existing_id(output_file: str, current_data: Dict[str, Any]) -> bool:
    """检查已有文件的ID是否与当前数据ID重复"""
    try:
        # 读取已有文件
        with open(output_file, "r", encoding="utf-8") as f:
            existing_data = json.load(f)

        existing_id = _extract_id_from_data(existing_data, "已有文件")
        current_id = _extract_id_from_data(current_data, "当前数据")

        # 对比ID
        if existing_id == current_id:
            return True
        else:
            logger.warning(f"模型ID不匹配（已有：{existing_id}，当前：{current_id}），将覆盖文件")
            return False

    except json.JSONDecodeError:
        logger.warning(f"已有文件格式错误（非标准JSON），将覆盖文件")
        return False
    except Exception as e:
        logger.warning(f"校验ID时出错：{str(e)}，将覆盖文件")
        return False


def _extract_id_from_data(data: Any, data_type: str) -> str:
    """从数据（列表/字典）中提取ID，无ID则抛异常"""
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
    new_id = new_item.get("ID")
    if not new_id:
        raise ValueError("待添加的数据缺少'ID'字段，无法进行唯一性校验")

    if new_id in existing_ids:
        logger.info(f"发现重复ID：{new_id}，已跳过该数据")
        return False
    else:
        target_list.append(new_item)
        existing_ids.add(new_id)
        return True


def generate_metrics_data(target_date: str = None) -> List[Dict[str, Any]]:
    """
    输出：ES写入 + 本地总表数据（JSON）
    """
    total_data: List[Dict[str, Any]] = []  # 本地总表数据
    total_existing_ids: Set[str] = set()  # 总表去重标识
    es_success_count: int = 0  # 统计ES写入成功次数
    es_fail_count: int = 0  # 统计ES写入失败次数
    all_valid_metrics: List[Dict[str, Any]] = []

    logger.info(f"=== 开始生成metrics数据（目标日期：{target_date}）===")
    try:
        current_date_str = get_date_str(target_date)
        date_dir_full = os.path.join(ROOT_DIR, current_date_str)
        commit_ids = get_subdir_names(date_dir_full)

        if not commit_ids:
            logger.info(f"日期目录 {date_dir_full} 下无commit_id子目录，终止处理")
            return total_data

        # ES初始化
        es_handler, es_index_name = es_operation.init_es_handler()
        if es_handler:
            logger.info(f"ES连接初始化成功，索引：{es_index_name}")
        else:
            logger.info("ES连接初始化失败，仅保留本地总表数据")

        # 遍历顺序为commit_id → model_name → request_rate
        for commit_id in commit_ids:
            logger.info(f"===== 处理 commit_id：{commit_id} =====")
            try:
                # 遍历commit_id目录
                commit_dir_full = os.path.join(date_dir_full, commit_id)
                model_names = get_subdir_names(commit_dir_full)

                if not model_names:
                    logger.info(f"commit_id {commit_id} 下无model_name子目录，跳过")
                    continue

                # 遍历model_name
                for model_name in model_names:
                    logger.info(f"----- 处理 model_name：{model_name}（commit：{commit_id}）-----")
                    model_dir_full = os.path.join(commit_dir_full, model_name)
                    request_rate_dirs = get_subdir_names(model_dir_full)

                    if not request_rate_dirs:
                        logger.info(f"model_name {model_name} 下无request_rate子目录，跳过")
                        continue

                    # 遍历request_rate
                    for request_rate in request_rate_dirs:
                        logger.info(f"--- 处理 request_rate：{request_rate}（model：{model_name}）---")

                        # 文件校验
                        is_file_valid, missing_files, file_paths = check_model_files(
                            current_date_str,
                            commit_id,
                            model_name,
                            request_rate
                        )
                        if not is_file_valid:
                            logger.info(f"组合 {model_name}@{request_rate} 跳过：缺少文件 → {', '.join(missing_files)}")
                            continue

                        # 数据生成与写入
                        try:
                            current_data = generate_single_model_data(model_name, file_paths)
                            if not current_data or "ID" not in current_data:
                                raise ValueError("数据为空或缺少必填字段'ID'")
                            data_id = current_data["ID"]

                            # ES写入
                            if es_handler:
                                logger.info(f"正在写入ES：ID={data_id}")
                                es_write_success = es_handler.add_data(
                                    index_name=es_index_name,
                                    doc_id=data_id,
                                    data=current_data
                                )
                                if es_write_success:
                                    es_success_count += 1
                                    logger.info(f"写入成功：ID={data_id}")
                                else:
                                    es_fail_count += 1
                                    logger.info(f"写入失败：ID={data_id}")

                            # 本地总表
                            if ensure_unique_id(total_data, current_data, total_existing_ids):
                                all_valid_metrics.append(current_data)
                                logger.info(f"加入本地总表：ID={data_id}（总表当前条数：{len(total_data)}）")
                            else:
                                logger.info(f"本地总表已存在该数据：ID={data_id}，跳过")

                        except Exception as e:
                            logger.info(f"request_rate {request_rate} 处理失败：{str(e)}，继续下一个")
                            continue

            except Exception as e:
                logger.warning(f"commit_id {commit_id} 处理异常：{str(e)}，继续下一个")
                continue

        # 本地总表写入与校验
        if total_data:
            total_data_path = os.path.join(ROOT_DIR, f"total_metrics_{current_date_str}.json")
            with open(total_data_path, "w", encoding="utf-8") as f:
                json.dump(total_data, f, ensure_ascii=False, indent=2)
            logger.info(f"本地总表数据已保存：{total_data_path}（共{len(total_data)}条）")
        else:
            logger.warning("无有效数据，本地总表文件未生成")

        logger.info(f"\n===== 数据处理结果校验 =====")
        logger.info(f"本地总表数据量：{len(total_data)} 条")
        if es_handler:
            logger.info(f"ES写入成功：{es_success_count} 条，失败：{es_fail_count} 条")
        logger.info(f"有效数据总量：{len(all_valid_metrics)} 条")

    except Exception as e:
        logger.warning(f"全局处理异常：{str(e)}，已保留已处理的总表数据")
        if total_data:
            total_data_path = os.path.join(ROOT_DIR, f"total_metrics_{current_date_str}_error.json")
            with open(total_data_path, "w", encoding="utf-8") as f:
                json.dump(total_data, f, ensure_ascii=False, indent=2)
            logger.info(f"异常时已保存部分总表数据：{total_data_path}")

    logger.info(f"=== 处理完成！===")
    return total_data

# ---------------------- 函数调用（主入口） ----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="传入目标日期（格式：YYYYMMDD）")

    parser.add_argument(
        "target_date",
        nargs="?",
        default=None,
        help="目标日期，格式为 YYYYMMDD（例如 20251023，默认：20251022）"
    )

    args = parser.parse_args()

    generate_metrics_data(target_date=args.target_date)

