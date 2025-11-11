FROM python:3.11

ENV DEBIAN_FRONTEND=noninteractive TZ=Asia/Shanghai
ENV PIP_PROGRESS_BAR=off
ENV PIP_NO_CACHE_DIR=1
ENV PYTHONUNBUFFERED=1

# 修复线程环境变量
ENV OPENBLAS_NUM_THREADS=1
ENV OMP_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
ENV VECLIB_MAXIMUM_THREADS=1
ENV NUMEXPR_NUM_THREADS=1

WORKDIR /app

# 复制requirements文件以利用Docker缓存
COPY requirements.txt .

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        util-linux curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN python3.11 -m pip config set global.index-url https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple && \
    python3.11 -m pip install apscheduler && \
    python3.11 -m pip install --upgrade pip && \
    python3.11 -m pip install -r requirements.txt

# 复制应用代码
COPY . .

RUN groupadd -g 1000 appgroup && \
    useradd -m -u 1000 -g appgroup appuser && \
    chown -R appuser:appgroup /app

EXPOSE 5000
ENV TZ=Asia/Shanghai

USER appuser

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5000/ || exit 1

# 使用exec确保gunicorn成为PID 1进程
CMD ["sh", "-c", "python3.11 /app/scheduler.py & exec gunicorn --bind 0.0.0.0:5000 --access-logfile - --error-logfile - app:app"]