# run.py
import os,sys
sys.path.append(os.getcwd())
import subprocess
import argparse
from src.settings import Settings
from src.util import (
    delete_hdfs_path,
    create_hdfs_path,
    upload_to_hdfs,
    download_hdfs_file,
    build_logger
)

logger = build_logger()

hadoop_streaming_path = Settings.basic_settings.hadoop_streaming_path
ckg4j_root = Settings.CKG4J_ROOT

def run(
    input_dir:str,
    output_dir:str,
    overwrite:bool=False
):
    create_hdfs_path(input_dir)
    
    # 上传输入文件
    if os.path.isdir(input_dir):
        for file in os.listdir(input_dir):
        # 如果是文件就上传
            print(file)
            file_path = f'{input_dir}/{file}'
            if os.path.isfile(file_path):
                upload_to_hdfs(file_path, file_path, overwrite)
    elif os.path.isfile(input_dir):
        upload_to_hdfs(input_dir, input_dir, overwrite)
    
    delete_hdfs_path(output_dir)

    # 构建Hadoop命令
    
    hadoop_cmd = f"""
hadoop jar {hadoop_streaming_path} \
    -D stream.map.output.field.separator=, \
    -D stream.num.map.output.key.fields=1 \
    -input {input_dir} \
    -output {output_dir} \
    -mapper 'python {ckg4j_root}/src/task3/mapper.py' \
    -reducer 'python {ckg4j_root}/src/task3/reducer.py' \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner
"""
    # 运行Hadoop命令
    try:
        result = subprocess.run(
            hadoop_cmd,
            shell=True,
            check=True,
            stderr=subprocess.PIPE,
            text=True
        )
        # 运行成功后，将输出文件夹下载到本地
        os.makedirs(output_dir,exist_ok=True)
        local_path = os.path.dirname(output_dir)
        download_hdfs_file(output_dir,local_path)
        logger.info(f"Task3 finished successfully, output_dir: {local_path}")
    except:
        logger.error(f"Error running hadoop command: {hadoop_cmd}")
        return
    
if __name__ == "__main__":
    logger.info(f"Start running task3, ckg4j_root: {ckg4j_root}, hadoop_streaming_path: {hadoop_streaming_path}")
    parser = argparse.ArgumentParser(description="Run Hadoop Task1")
    parser.add_argument("--input_dir",  help="Path to the input files directory")
    parser.add_argument("--output_dir", default="task3/output", help="HDFS output dir")
    parser.add_argument("--overwrite", action='store_true' , help="Overwrite existing files on HDFS")
    args = parser.parse_args()
    logger.info(f"Task3 args: {args._get_args()}")
    
    run(
        f'{ckg4j_root}/{args.input_dir}',
        f'{ckg4j_root}/{args.output_dir}',
        args.overwrite
    )