FROM python:3.11
ENV DEBIAN_FRONTEND=noninteractive TZ=Asia/Shanghai
ENV PIP_PROGRESS_BAR=off
ENV PIP_NO_CACHE_DIR=1
ENV PYTHONUNBUFFERED=1

ENV OPENBLAS_NUM_THREADS=1
ENV OMP_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
ENV VECLIB_MAXINUM_THREADS=1
ENV NUMEXPR_NUM_THREADS=1

WORKDIR /app

COPY . .

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        util-linux && \
    apt-get clean  && \
    rm -rf /var/lib/apt/lists/*

    
RUN python3.11 -m pip config set global.index-url https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple && \
    python3.11 -m pip install --upgrade pip --no-cache-dir && \
    python3.11 -m pip install apscheduler && \
    python3.11 -m pip install -r requirements.txt --no-cache-dir --no-deps

RUN groupadd -g 1000 appgroup 
RUN useradd -m -u 1000 -g appgroup appuser
RUN chown -R appuser:appgroup /app

EXPOSE 5000
ENV TZ=Asia/Shanghai

USER appuser
CMD ["sh", "-c", "python /app/scheduler.py & gunicorn --bind 0.0.0.0:5000 app:app & wait"]
