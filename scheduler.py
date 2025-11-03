import subprocess
from apscheduler.schedulers.blocking import BlockingScheduler
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
            ["python3", "-m", "data.data_processor"],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info(f"data_processor执行成功：{result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"data_processor执行失败：{e.stderr}")
    except Exception as e:
        logging.error(f"执行过程发生未知错误：{str(e)}")

if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(
        run_data_processor,
        "cron",
        hour="3,21",  # 3点和21点
        minute="0",
        second="0"
    )

    logging.info("定时任务调度器启动，将在每天3:00和21:00执行data_processor.py")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("调度器已停止")
