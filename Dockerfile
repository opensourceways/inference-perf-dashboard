FROM python:3.11

WORKDIR /app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt --timeout 100 -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*
RUN groupadd -g 1000 appgroup
RUN useradd -m -u 1000 -g appgroup appuser
RUN chown -R appuser:appgroup /app

# 配置定时任务（每天凌晨3点执行 data_processor.py）
RUN echo "0 3 * * * root su - appuser -c 'python /app/data/data_processor.py'" > /etc/cron.d/daily_processor
RUN chmod 0644 /etc/cron.d/daily_processor
RUN crontab /etc/cron.d/daily_processor

EXPOSE 5000
ENV TZ=Asia/Shanghai

RUN echo "#!/bin/bash\ncron -f &\ngunicorn --bind 0.0.0.0:5000 app:app" > /app/start.sh && chmod +x /app/start.sh

USER appuser
CMD ["/app/start.sh"]
