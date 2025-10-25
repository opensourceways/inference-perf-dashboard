# 1 创建索引及映射

# 替换 <你的密码> 为elastic用户密码
curl -u elastic:<你的密码> -k -X PUT "https://localhost:9200/model_performance" \
  -H "Content-Type: application/json" \
  -d '{
    "mappings": {
      "properties": {
        "ID": {"type": "keyword"},  // 唯一标识，精确匹配
        "source": {
          "properties": {
            "pr_id": {"type": "keyword"},
            "commit_id": {"type": "keyword"},
            "commit_title": {"type": "text"},  // 文本类型，支持全文检索
            "created_at": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss"},  // 日期类型
            "sglang_branch": {"type": "keyword"},
            "model_name": {"type": "keyword"},  // 模型名，适合筛选
            "device": {"type": "keyword"},
            "mean_e2e1_ms": {"type": "float"},  // 浮点型（延迟指标）
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
            "max_concurrency": {"type": "integer"},  // 整数型（并发数）
            "request_throughput": {"type": "float"},  // 吞吐量（浮点）
            "total_input_tokens": {"type": "integer"},  // token数（整数）
            "total_generated_tokens": {"type": "integer"},
            "input_token_throughput": {"type": "float"},
            "output_token_throughput": {"type": "float"},
            "total_token_throughput": {"type": "float"}
          }
        }
      }
    }
  }'

# curl命令写入
# 替换 <你的密码> 和数据中的ID（作为文档_id）
curl -u elastic:<你的密码> -k -X PUT "https://localhost:9200/model_performance/_doc/la2b3c4d_Qwen3-32B" \
  -H "Content-Type: application/json" \
  -d '{
    "ID": "la2b3c4d_Qwen3-32B",
    "source": {
      "pr_id": "123456",
      "commit_id": "la2b3c4d",
      "commit_title": "This is a test.",
      "created_at": "2025-10-22T15:20:00",
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
  }'

# python脚本写入
pip install elasticsearch

from elasticsearch import Elasticsearch

# 连接ES（替换<你的密码>）
es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "<你的密码>"),
    verify_certs=False  # 开发环境跳过证书验证
)

# 你的数据
data = {
    "ID": "la2b3c4d_Qwen3-32B",
    "source": {
        "pr_id": "123456",
        "commit_id": "la2b3c4d",
        "commit_title": "This is a test.",
        "created_at": "2025-10-22T15:20:00",
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

# 写入数据（指定文档ID为data["ID"]）
response = es.index(
    index="model_performance",  # 索引名
    id=data["ID"],              # 文档ID（与数据中的ID一致）
    document=data               # 要写入的JSON数据
)

print("写入结果：", response["result"])  # 输出 "created" 表示成功

# 数据验证
# 替换 <你的密码> 和文档ID（la2b3c4d_Qwen3-32B）
curl -u elastic:<你的密码> -k "https://localhost:9200/model_performance/_doc/la2b3c4d_Qwen3-32B"

# 接上面的Python代码，查询刚写入的文档
response = es.get(index="model_performance", id="la2b3c4d_Qwen3-32B")
print("查询结果：", response["_source"])  # 输出完整数据
