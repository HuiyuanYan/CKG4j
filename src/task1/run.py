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
    name_dict_path:str,
    input_dir:str,
    output_dir:str,
    overwrite:bool
):

    create_hdfs_path(os.path.dirname(name_dict_path))
    create_hdfs_path(input_dir)
    
    # 上传人名列表
    upload_to_hdfs(name_dict_path, name_dict_path, overwrite)
    
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
    -mapper 'python {ckg4j_root}/src/task1/mapper.py nameList' \
    -reducer 'python {ckg4j_root}/src/task1/reducer.py' \
    -input {input_dir} \
    -output {output_dir} \
    -cacheFile {name_dict_path}#nameList
"""
    # 运行Hadoop命令
    try:
        subprocess.run(hadoop_cmd, shell=True, check=True)
        # 运行成功后，将输出文件夹下载到本地
        os.makedirs(output_dir,exist_ok=True)
        local_path = os.path.dirname(output_dir)
        download_hdfs_file(output_dir,local_path)
        logger.info(f"Task1 finished successfully, output_dir: {local_path}")
    except:
        logger.error(f"Error running hadoop command: {hadoop_cmd}")
        return
    
if __name__ == "__main__":
    logger.info(f"Start running task1, ckg4j_root: {ckg4j_root}, hadoop_streaming_path: {hadoop_streaming_path}")
    parser = argparse.ArgumentParser(description="Run Hadoop Task1")
    parser.add_argument("--name_list_path", default="data/test/character.json", help="Path to the name list file")
    parser.add_argument("--input_dir",  help="Path to the input files directory")
    parser.add_argument("--output_dir", default="task1/output", help="HDFS output dir")
    parser.add_argument("--overwrite", action='store_true' , help="Overwrite existing files on HDFS")
    args = parser.parse_args()
    logger.info(f"Task1 args: {args._get_args()}")
    run(
        f'{ckg4j_root}/{args.name_list_path}',
        f'{ckg4j_root}/{args.input_dir}',
        f'{ckg4j_root}/{args.output_dir}',
        args.overwrite
    )