from es_command import es_operation


if __name__ == "__main__":
    # 初始化ESHandler实例
    es_handler, es_index_name = es_operation.init_es_handler()

    # 1. 创建索引（使用默认映射）
    es_handler.create_index(es_index_name)

    # 2. 准备测试数据
    test_data = {
        "ID": "la2b3c4d_Qwen3-32B",
        "source": {
            "pr_id": "123456",
            "commit_id": "la2b3c4d",
            "pr_title": "This is a test.",
            "merged_at": "2025-10-22T15:20:00",
            "sglang_branch": "main",
            "model_name": "Qwen3-32B",
            "device": "Ascend910B3",
            "mean_e2e1_ms": 2801.1999,
            "mean_ttft_ms": 45.0018,
            "mean_tpot_ms": 16.5773,
            "mean_itl_ms": 17.0079,
            "p99_e2e1_ms": 8979.7446,
            "p99_ttft_ms": 48.4629,
            "p99_tpot_ms": 18.222,
            "p99_itl_ms": 19.4617,
            "median_e2e1_ms": 1751.5196,
            "median_ttft_ms": 44.5277,
            "median_tpot_ms": 16.2571,
            "median_itl_ms": 16.445,
            "max_concurrency": 1,
            "request_throughput": 0.357,
            "total_input_tokens": 10759,
            "total_generated_tokens": 1126,
            "input_token_throughput": 548.637,
            "output_token_throughput": 57.4185,
            "total_token_throughput": 606.0555
        }
    }
    doc_id = test_data["ID"]

    # 3. 添加数据
    es_handler.add_data(es_index_name, doc_id, test_data)

    # 4. 检查ID是否存在
    print(f"ID存在性检查：{es_handler.check_id_exists(es_index_name, doc_id)}")

    # 5. 查询数据
    data = es_handler.get_data(es_index_name, doc_id)
    if data:
        print("查询到的数据：", data)

    # 6. 修改数据（示例：更新mean_e2e1_ms字段）
    es_handler.update_data(
        index_name=es_index_name,
        doc_id=doc_id,
        update_fields={"source.mean_e2e1_ms": 3000.0}  # 只更新指定字段
    )

    # 7. 再次查询，验证修改结果
    updated_data = es_handler.get_data(es_index_name, doc_id)
    if updated_data:
        print("修改后的数据（mean_e2e1_ms）：", updated_data["source"]["mean_e2e1_ms"])

    # 8. 删除数据
    # es_handler.delete_data(ES_CONFIG["index_name"], doc_id)