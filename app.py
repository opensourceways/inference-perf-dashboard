import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import gevent
import numpy as np
import pandas as pd
import requests
import yaml

from flask import Flask, jsonify, render_template, request, send_from_directory
from flask_cors import CORS


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

os.environ.pop('HTTP_PROXY', None)
os.environ.pop('HTTPS_PROXY', None)
os.environ.pop('http_proxy', None)
os.environ.pop('https_proxy', None)

app = Flask(__name__)
CORS(app)

with open("config/config.yaml", "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)

commit_url = config
commit_url = config["commit"]["url"]
serving_url = config["es"]["url"] + "/" + config["es"]["serving"]
throughput_url = config["es"]["url"] + "/" + config["es"]["throughput"]
latency_url = config["es"]["url"] + "/" + config["es"]["latency"]
serving_url_v1 = config["es"]["url"] + "/" + config["es"]["serving_v1"]
throughput_url_v1 = config["es"]["url"] + "/" + config["es"]["throughput_v1"]
latency_url_v1 = config["es"]["url"] + "/" + config["es"]["latency_v1"]
volatility_url = config["es"]["url"] + "/" + config["es"]["volatility"]
headers = {
    "Content-Type": "application/x-ndjson",
    "Authorization": config["es"]["token"]
}
payload = {"_source": True, "size": 10000, "query": {"match_all": {}}}
proxies = {}

session = requests.Session()
session.trust_env = False

def extract_v1(ef):
    if isinstance(ef, dict):
        return ef.get("VLLM_USE_V1", "0")
    return "0"

@app.route('/health')
def health_check():
    return jsonify(status="healthy"), 200

@app.route('/commits/list')
def get_commit_list():
    models = request.args.get('models', type=str)
    start_time = request.args.get('startTime', type=int)
    end_time = request.args.get('endTime', type=int)
    engine_version = request.args.get("engineVersion", default="0", type=str)
    response = session.post(throughput_url_v1,
                            headers=headers,
                            json=payload,
                            verify=False,
                            proxies=proxies)
    result = response.json()
    hits = result.get("hits", {}).get("hits", {})
    records = [hit.get("_source", {}) for hit in hits]
    df = pd.DataFrame(records)
    df["VLLM_USE_V1"] = df["extra_features"].apply(extract_v1)
    df = df[df["VLLM_USE_V1"] == engine_version]
    df['created_at'] = pd.to_datetime(df['created_at'],
                                      format='%Y-%m-%dT%H:%M:%S',
                                      errors='coerce')
    df['time'] = (df['created_at'] -
                  pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
    df = df[["model_name", "commit_id", "time",
             "vllm_ascend_branch"]].rename(columns={
                 "commit_id": "hash",
                 "vllm_ascend_branch": "branch"
             })
    if models != "all":
        df = df[df['model_name'] == models]
    df = df.sort_values(by="time", ascending=False)
    df = df[df['time'].between(start_time, end_time, inclusive='both')]
    if df.empty:
        return jsonify({})
    df['device'] = "Altlas A3"
    grouped: dict = (df.groupby('model_name').apply(
        lambda g: g[['device', 'branch', 'hash', 'time']].to_dict(
            orient='records')).to_dict())
    return jsonify(grouped)


@app.route('/data-details/list')
def get_data_list():
    models = request.args.get('models', type=str)
    start_time = request.args.get('startTime', type=int)
    end_time = request.args.get('endTime', type=int)
    engine_version = request.args.get("engineVersion", default="0", type=str)
    jobs = [
        gevent.spawn(session.post,
                     latency_url_v1,
                     headers=headers,
                     json=payload,
                     verify=False,
                     proxies=proxies),
        gevent.spawn(session.post,
                     serving_url_v1,
                     headers=headers,
                     json=payload,
                     verify=False,
                     proxies=proxies),
        gevent.spawn(session.post,
                     throughput_url_v1,
                     headers=headers,
                     json=payload,
                     verify=False,
                     proxies=proxies)
    ]
    gevent.joinall(jobs)
    response = []
    for job in jobs:
        hits = job.value.json().get("hits", {}).get("hits", {})
        records = [hit.get("_source", {}) for hit in hits]
        response.append(records)
    # throughput
    df3 = pd.DataFrame(response[2])
    df3 = df3[df3["status"] != "skip"]
    df3["VLLM_USE_V1"] = df3["extra_features"].apply(extract_v1)
    df3 = df3[df3["VLLM_USE_V1"] == engine_version]
    df3 = df3[df3['tp'] == 1]
    if models != "all":
        df3 = df3[df3['model_name'] == models]
    df3['created_at'] = pd.to_datetime(df3['created_at'],
                                       format='%Y-%m-%dT%H:%M:%S',
                                       errors='coerce')
    df3['time'] = (df3['created_at'] -
                   pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
    df3 = df3.sort_values(by="time", ascending=False)
    df3 = df3[df3['time'].between(start_time, end_time, inclusive='both')]
    df3 = df3[[
        "time", "model_name", "commit_id", "status", "requests_per_second",
        "tokens_per_second"
    ]].rename(columns={"commit_id": "hash"})
    if df3.empty:
        return jsonify({})
    # latency
    df1 = pd.DataFrame(response[0])
    df1 = df1[df1["status"] != "skip"]
    df1["VLLM_USE_V1"] = df1["extra_features"].apply(extract_v1)
    df1 = df1[df1["VLLM_USE_V1"] == engine_version]
    df1 = df1[["model_name", "commit_id", "mean_latency"]].rename(columns={
        "commit_id": "hash",
        "mean_latency": "latency"
    })
    df1['latency'] = (df1['latency'] / 1000).round(2)
    # serving
    df2 = pd.DataFrame(response[1])
    df2 = df2[df2["status"] != "skip"]
    df2["VLLM_USE_V1"] = df2["extra_features"].apply(extract_v1)
    df2 = df2[df2["VLLM_USE_V1"] == engine_version]
    df2 = df2[df2['tp'] == 1]
    df2 = df2[[
        "model_name", "commit_id", "request_rate", "mean_itl_ms",
        "mean_tpot_ms", "mean_ttft_ms", "p99_itl_ms", "p99_tpot_ms",
        "p99_ttft_ms", "request_throughput", "output_throughput",
        "total_token_throughput"
    ]].rename(
        columns={
            "commit_id": "hash",
            "request_rate": "qps",
            "request_throughput": "request_throughput_serve_per_sec",
            "output_throughput": "output_throughput_serve_per_sec",
            "total_token_throughput": "total_token_throughput_per_sec"
        })
    merge1 = pd.merge(df3, df2, on=['hash', 'model_name'], how='left')
    merge2 = pd.merge(merge1, df1, on=['hash', 'model_name'], how='left')
    merge2 = merge2.round(2)
    result_json = merge2.to_json(orient='records', force_ascii=False, indent=2)
    return result_json


@app.route('/data-details-compare/list')
def get_data_compass_list():
    models = request.args.get('models', type=str)
    start_time = request.args.get('startTime', type=int)
    end_time = request.args.get('endTime', type=int)
    engine_version = request.args.get("engineVersion", default="0", type=str)
    sort_type = False
    if start_time > end_time:
        sort_type = True
        start_time, end_time = end_time, start_time
    jobs = [
        gevent.spawn(session.post,
                     latency_url_v1,
                     headers=headers,
                     json=payload,
                     verify=False,
                     proxies=proxies),
        gevent.spawn(session.post,
                     serving_url_v1,
                     headers=headers,
                     json=payload,
                     verify=False,
                     proxies=proxies),
        gevent.spawn(session.post,
                     throughput_url_v1,
                     headers=headers,
                     json=payload,
                     verify=False,
                     proxies=proxies)
    ]
    gevent.joinall(jobs)
    response = []
    for job in jobs:
        hits = job.value.json().get("hits", {}).get("hits", {})
        records = [hit.get("_source", {}) for hit in hits]
        response.append(records)

    # throughput
    df3 = pd.DataFrame(response[2])
    df3 = df3[df3["status"] == "normal"]
    df3["VLLM_USE_V1"] = df3["extra_features"].apply(extract_v1)
    df3 = df3[df3["VLLM_USE_V1"] == engine_version]
    if models != "all":
        df3 = df3[df3['model_name'] == models]
    df3['device'] = "Altlas A2"
    df3['created_at'] = pd.to_datetime(df3['created_at'],
                                       format='%Y-%m-%dT%H:%M:%S',
                                       errors='coerce')
    df3['time'] = (df3['created_at'] -
                   pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
    df3 = df3.sort_values(by="time", ascending=sort_type)
    df3 = df3[df3['time'].between(start_time, end_time, inclusive='both')]
    df3 = df3.round(2)
    agg = df3.groupby(['model_name']).agg(
        requests_per_second_start=('requests_per_second', 'first'),
        requests_per_second_end=('requests_per_second', 'last'),
        tokens_per_second_start=('tokens_per_second', 'first'),
        tokens_per_second_end=('tokens_per_second', 'last'),
        commit_id=('commit_id', 'first'),
        tp=('tp', 'first'),
        size=('model_name', 'size')).reset_index()
    for col in ['requests_per_second', 'tokens_per_second']:
        agg[f'{col}'] = np.where(
            agg['size'] > 1, agg[f'{col}_end'].astype(str) + '→' +
            agg[f'{col}_start'].astype(str), agg[f'{col}_start'].astype(str))
    df3_agg = agg[[
        'model_name', 'commit_id', 'tp', 'requests_per_second',
        'tokens_per_second'
    ]]
    df3_agg = agg.assign(
        device="Altlas A2",
        request_rate=np.nan,
        mean_itl_ms=np.nan,
        mean_tpot_ms=np.nan,
        mean_ttft_ms=np.nan,
        p99_itl_ms=np.nan,
        p99_tpot_ms=np.nan,
        p99_ttft_ms=np.nan,
        serve_request_throughput_req_s=np.nan,
        serve_output_throughput_tok_s=np.nan,
        serve_total_throughput_tok_s=np.nan,
    )[[
        'model_name',
        'commit_id',
        'tp',
        'device',
        'request_rate',
        'mean_itl_ms',
        'mean_tpot_ms',
        'mean_ttft_ms',
        'p99_itl_ms',
        'p99_tpot_ms',
        'p99_ttft_ms',
        'serve_request_throughput_req_s',
        'serve_output_throughput_tok_s',
        'serve_total_throughput_tok_s',
        'requests_per_second',
        'tokens_per_second',
    ]]

    df3_agg = df3_agg.rename(
        columns={
            'model_name': 'name',
            'commit_id': 'hash',
            'tp': 'tensor_parallel',
            'requests_per_second': 'requests_req_s',
            'tokens_per_second': 'tokens_tok_s',
        })

    # latency
    df1 = pd.DataFrame(response[0])
    if models != "all":
        df1 = df1[df1['model_name'] == models]
    df1 = df1[df1["status"] == "normal"]
    df1["VLLM_USE_V1"] = df1["extra_features"].apply(extract_v1)
    df1 = df1[df1["VLLM_USE_V1"] == engine_version]
    if not df1.empty:
        df1 = df1[df1["status"] == "normal"]
        df1['created_at'] = pd.to_datetime(df1['created_at'],
                                           format='%Y-%m-%dT%H:%M:%S',
                                           errors='coerce')
        df1['time'] = (df1['created_at'] -
                       pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
        df1 = df1[["model_name", "tp", "mean_latency", "time", "commit_id"]]
        df1['latency'] = (df1['mean_latency'] / 1000).round(2)
        df1 = df1.sort_values(by="time", ascending=sort_type)
        df1 = df1[df1['time'].between(start_time, end_time, inclusive='both')]
        df1 = df1.round(2)
        agg = df1.groupby(['model_name'
                           ]).agg(latency_start=('latency', 'first'),
                                  latency_end=('latency', 'last'),
                                  commit_id=('commit_id', 'first'),
                                  tp=('tp', 'first'),
                                  size=('model_name', 'size')).reset_index()
        agg[f'latency_s'] = np.where(
            agg['size'] > 1, agg[f'latency_end'].astype(str) + '→' +
            agg[f'latency_start'].astype(str),
            agg[f'latency_start'].astype(str))
        df1_agg = agg[[
            'model_name',
            'commit_id',
            'latency_s',
        ]]
        df1_agg = df1_agg.rename(columns={
            'model_name': 'name',
            'commit_id': 'hash',
        })

    # serving
    df2 = pd.DataFrame(response[1])
    if models != "all":
        df2 = df2[df2['model_name'] == models]
    df2 = df2[df2["status"] == "normal"]
    df2["VLLM_USE_V1"] = df2["extra_features"].apply(extract_v1)
    df2 = df2[df2["VLLM_USE_V1"] == engine_version]
    df2['created_at'] = pd.to_datetime(df2['created_at'],
                                       format='%Y-%m-%dT%H:%M:%S',
                                       errors='coerce')
    df2['time'] = (df2['created_at'] -
                   pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
    df2 = df2.sort_values(by="time", ascending=sort_type)
    df2 = df2[df2['time'].between(start_time, end_time, inclusive='both')]
    df2 = df2[[
        "model_name", "time", "tp", "request_rate", "commit_id",
        "mean_itl_ms", "mean_tpot_ms", "mean_ttft_ms", "p99_itl_ms",
        "p99_tpot_ms", "p99_ttft_ms", "request_throughput",
        "output_throughput", "total_token_throughput"
    ]]
    df2 = df2.round(2)
    agg = df2.groupby(['model_name', 'request_rate']).agg(
        itl_start=('mean_itl_ms', 'first'),
        itl_end=('mean_itl_ms', 'last'),
        tpot_start=('mean_tpot_ms', 'first'),
        tpot_end=('mean_tpot_ms', 'last'),
        ttft_start=('mean_ttft_ms', 'first'),
        ttft_end=('mean_ttft_ms', 'last'),
        p99_itl_start=('p99_itl_ms', 'first'),
        p99_itl_end=('p99_itl_ms', 'last'),
        p99_tpot_start=('p99_tpot_ms', 'first'),
        p99_tpot_end=('p99_tpot_ms', 'last'),
        p99_ttft_start=('p99_ttft_ms', 'first'),
        p99_ttft_end=('p99_ttft_ms', 'last'),
        request_throughput_start=('request_throughput', 'first'),
        request_throughput_end=('request_throughput', 'last'),
        output_throughput_start=('output_throughput', 'first'),
        output_throughput_end=('output_throughput', 'last'),
        total_token_throughput_start=('total_token_throughput', 'first'),
        total_token_throughput_end=('total_token_throughput', 'last'),
        commit_id=('commit_id', 'first'),
        tp=('tp', 'first'),
        size=('model_name', 'size')).reset_index()

    for col in [
            'itl', 'tpot', 'ttft', 'p99_itl', 'p99_tpot', 'p99_ttft',
            'request_throughput', 'output_throughput', 'total_token_throughput'
    ]:
        agg[f'{col} (ms)'] = np.where(
            agg['size'] > 1, agg[f'{col}_end'].astype(str) + '→' +
            agg[f'{col}_start'].astype(str), agg[f'{col}_start'].astype(str))
    df2_agg = agg[[
        'model_name',
        'commit_id',
        'tp',
        'request_rate',
        'itl (ms)',
        'tpot (ms)',
        'ttft (ms)',
        'p99_itl (ms)',
        'p99_tpot (ms)',
        'p99_ttft (ms)',
    ]]
    df2_agg = agg.assign(
        device="Altlas A2",
        latency=np.nan,
        requests_per_second=np.nan,
        tokens_per_second=np.nan,
    )[[
        'model_name',
        'commit_id',
        'tp',
        'request_rate',
        'device',
        'latency',
        'itl (ms)',
        'tpot (ms)',
        'ttft (ms)',
        'p99_itl (ms)',
        'p99_tpot (ms)',
        'p99_ttft (ms)',
        'request_throughput (ms)',
        'output_throughput (ms)',
        'total_token_throughput (ms)',
        'requests_per_second',
        'tokens_per_second',
    ]]
    df2_agg = df2_agg.rename(
        columns={
            'model_name': 'name',
            'commit_id': 'hash',
            'tp': 'tensor_parallel',
            'request_rate': 'request_rate',
            'device': 'device',
            'latency': 'latency_s',
            'itl (ms)': 'mean_itl_ms',
            'tpot (ms)': 'mean_tpot_ms',
            'ttft (ms)': 'mean_ttft_ms',
            'p99_itl (ms)': 'p99_itl_ms',
            'p99_tpot (ms)': 'p99_tpot_ms',
            'p99_ttft (ms)': 'p99_ttft_ms',
            'request_throughput (ms)': 'serve_request_throughput_req_s',
            'output_throughput (ms)': 'serve_output_throughput_tok_s',
            'total_token_throughput (ms)': 'serve_total_throughput_tok_s',
            'requests_per_second': 'requests_req_s',
            'tokens_per_second': 'tokens_tok_s',
        })

    if not df1.empty:
        merge1 = pd.merge(df3_agg, df1_agg, on=['name', 'hash'], how='left')
    else:
        df3_agg['latency_s'] = np.nan
        merge1 = df3_agg
    combined_all = pd.concat([merge1, df2_agg], ignore_index=True)
    result = (combined_all.groupby(
        ["name", "tensor_parallel", "device"]).apply(lambda x: x.sort_values(
            "request_rate",
            ascending=True,
            key=lambda col: pd.to_numeric(col, errors="coerce"),
            na_position='first')).reset_index(drop=True))

    desired_order = [
        "name",
        "tensor_parallel",
        "request_rate",
        "device",
        "latency_s",
        "mean_itl_ms",
        "mean_tpot_ms",
        "mean_ttft_ms",
        "p99_itl_ms",
        "p99_tpot_ms",
        "p99_ttft_ms",
        "serve_request_throughput_req_s",
        "serve_output_throughput_tok_s",
        "serve_total_throughput_tok_s",
        "requests_req_s",
        "tokens_tok_s",
    ]
    result = result[desired_order]
    result_json = result.to_json(orient='records', force_ascii=False, indent=2)
    return result_json

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
