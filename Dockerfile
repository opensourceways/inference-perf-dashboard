FROM python:3.11

WORKDIR /app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt --timeout 100 -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN groupadd -g 1000 appgroup
RUN useradd -m -u 1000 -g appgroup appuser
RUN chown -R appuser:appgroup /app

EXPOSE 5000
ENV TZ=Asia/Shanghai
USER appuser
# CMD ["gunicorn", "--bind", "0.0.0.0:5000", "app:app"]
CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:5000 app:app & wait"]