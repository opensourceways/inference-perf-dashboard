import subprocess
import time
from apscheduler.schedulers.background import BackgroundScheduler
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="/app/cron.log"
)


def run_data_processor():
    """执行data/data_processor.py的函数，将数据写入ES数据库"""
    try:
        result = subprocess.run(
            ["python3.11", "-m", "data.data_processor"],
            check=True,
            capture_output=True,
            text=True,
            timeout=3600  # 新增：1小时超时控制，避免无限阻塞
        )
        logging.info(f"data_processor执行成功：{result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"data_processor执行失败：{e.stderr}")
    except subprocess.TimeoutExpired:
        logging.error("data_processor执行超时（超过1小时）")
    except Exception as e:
        logging.error(f"执行过程发生未知错误：{str(e)}")


if __name__ == "__main__":
    scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(
        run_data_processor,
        "cron",
        hour="3,21",  # 3点和21点
        minute="0",
        second="0"
    )

    scheduler.start()  # 非阻塞启动，不会卡住进程
    logging.info("定时任务调度器启动，将在每天3:00和21:00执行data_processor.py")

    # 循环阻塞，保持进程存活（关键！）
    try:
        while True:
            time.sleep(60)  # 每分钟检查一次，防止进程退出
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()  # 退出时优雅关闭调度器
        logging.info("调度器已停止")
