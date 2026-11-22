FROM python:3.12-slim

WORKDIR /srv/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY templates ./templates

ENV DATA_DIR=/data \
    GUNICORN_STATE_DIR=/tmp/gunicorn \
    MAP_CODE_COL=local_code \
    MAP_NAME_COL=disease_name \
    DETAIL_CODE_COL=local_code \
    OUT_CODE_COL=code \
    PREPARE_DELAY_SECONDS=0

VOLUME /data
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8000/healthz')" || exit 1

# 单 worker 多线程: 后台准备线程与请求线程同进程; SQLite(WAL) 串行化发布事务
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "1", "--threads", "8", "--timeout", "300", "main:app"]
