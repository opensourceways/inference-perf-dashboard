import subprocess
import time
import signal
import sys
from apscheduler.schedulers.background import BackgroundScheduler
import logging

# 配置日志 - 同时输出到文件和控制台
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/app/cron.log"),
        logging.StreamHandler()  # 同时输出到控制台
    ]
)

logger = logging.getLogger(__name__)


def run_data_processor():
    """执行data/data_processor.py的函数，将数据写入ES数据库"""
    try:
        logger.info("开始执行data_processor...")
        result = subprocess.run(
            ["python3.11", "-m", "data.data_processor"],
            check=True,
            capture_output=True,
            text=True,
            timeout=3600
        )
        logger.info(f"data_processor执行成功：{result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"data_processor执行失败：{e.stderr}")
    except subprocess.TimeoutExpired:
        logger.error("data_processor执行超时（超过1小时）")
    except Exception as e:
        logger.error(f"执行过程发生未知错误：{str(e)}")


def signal_handler(signum, frame):
    """信号处理函数"""
    logger.info(f"接收到信号 {signum}，正在关闭调度器...")
    scheduler.shutdown()
    sys.exit(0)


if __name__ == "__main__":
    # 注册信号处理器
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(
        run_data_processor,
        "cron",
        hour="3,21",
        minute="0",
        second="0"
    )

    scheduler.start()
    logger.info("定时任务调度器启动，将在每天3:00和21:00执行data_processor.py")

    # 保持主线程活跃
    try:
        while scheduler.running:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("接收到退出信号")
    finally:
        if scheduler.running:
            scheduler.shutdown()
        logger.info("调度器已完全停止")