import os
import sys
from pathlib import Path
sys.path.append(os.getcwd())
from hdfs import InsecureClient
import loguru
from src.settings import Settings

def build_logger(log_file:str = 'ckg4j'):
    if log_file:
        if not log_file.endswith(".log"):
            log_file = f"{log_file}.log"
        if not os.path.isabs(log_file):
            log_file = Path(Settings.basic_settings.LOG_PATH/ log_file).resolve()
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logger = loguru.logger.opt(colors=True)
    logger.add(log_file, colorize=False)
    return logger

logger = build_logger()

hdfs_client = InsecureClient(
    url=Settings.basic_settings.hdfs_url,
    user=Settings.basic_settings.hdfs_user
)

logger.info(f"HDFS client info: url={Settings.basic_settings.hdfs_url}, user={Settings.basic_settings.hdfs_user}")

def create_hdfs_path(hdfs_path):
    try:
        # 检查HDFS路径是否存在，如果不存在则创建
        if not hdfs_client.status(hdfs_path, strict=False):
            logger.info(f"Creating HDFS path: {hdfs_path}")
            return hdfs_client.makedirs(hdfs_path, permission="755")
        else:
            logger.info(f"HDFS path already exists: {hdfs_path}")
    except Exception as e:
        logger.error(f"Failed to create HDFS path {hdfs_path}: {e}")

def upload_to_hdfs(local_path, hdfs_path, overwrite=False):
    try:
        
        # 检查HDFS上文件是否存在
        if hdfs_client.status(hdfs_path, strict=False) and not overwrite:
            logger.warning(f"File {hdfs_path} already exists and will not be overwritten.")
            return
        
        return hdfs_client.upload(hdfs_path=hdfs_path,local_path=local_path,overwrite=overwrite)
    except Exception as e:
        logger.error(f"Failed to upload file to HDFS {hdfs_path}: {e}")

def delete_hdfs_path(hdfs_path, recursive=True):
    try:
        return hdfs_client.delete(hdfs_path, recursive=recursive)
    except Exception as e:
        logger.error(f"Failed to delete HDFS path {hdfs_path}: {e}")

def download_hdfs_file(hdfs_path, local_path, overwrite: bool = True) -> str | None:
    try:
        # 检查HDFS上文件或文件夹是否存在
        if not hdfs_client.status(hdfs_path, strict=False):
            logger.warning(f"Path {hdfs_path} does not exist.")
            return
        return hdfs_client.download(hdfs_path, local_path, overwrite=overwrite)
    except Exception as e:
        logger.error(f"Failed to download from HDFS {hdfs_path} to local path {local_path}: {e}")

# 使用示例
if __name__ == "__main__":
    hdfs_path = '/data3/funing/CKG4j/task1/output'
    download_hdfs_file(hdfs_path, 'output')