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
    build_logger
)

logger = build_logger()

hadoop_streaming_path = Settings.basic_settings.hadoop_streaming_path
ckg4j_root = Settings.CKG4J_ROOT

def run(
    input_dir:str,
    output_dir:str,
    damping_factor:float=0.85,
    epsilon:float=1.0e-6,
    max_iterations:int=100,
    overwrite:bool=False,
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

    # 构建page_rank命令
    python_cmd = f"""
python {ckg4j_root}/src/task4/page_rank.py \
    --input_dir {input_dir} \
    --output_dir {output_dir} \
    --damping_factor {damping_factor} \
    --epsilon {epsilon} \
    --max_iterations {max_iterations}
    """    

    # 运行page_rank命令
    try:
        subprocess.run(python_cmd, shell=True, check=True)
        # 上传结果文件
        upload_to_hdfs(output_dir,output_dir,overwrite=True)
        logger.info(f"Task4 finished successfully, output_dir: {output_dir}")
    except:
        logger.error(f"Error running python command: {python_cmd}")
        return

if __name__ == "__main__":
    logger.info(f"Start running task4, ckg4j_root: {ckg4j_root}, hadoop_streaming_path: {hadoop_streaming_path}")
    parser = argparse.ArgumentParser(description="Run Hadoop Task1")
    parser.add_argument("--input_dir",  help="Path to the input files directory")
    parser.add_argument("--output_dir", default="task3/output", help="HDFS output dir")
    parser.add_argument("--overwrite", action='store_true' , help="Overwrite existing files on HDFS")
    parser.add_argument("--damping_factor", type=float, default=0.85, help="Damping factor for PageRank (default: 0.85)")
    parser.add_argument("--epsilon", type=float, default=1.0e-6, help="Epsilon for convergence (default: 1.0e-6)")
    parser.add_argument("--max_iterations", type=int, default=100, help="Maximum number of iterations (default: 100)")
    
    args = parser.parse_args()
    logger.info(f"Task4 args: {args._get_args()}")
    
    run(
        f'{ckg4j_root}/{args.input_dir}',
        f'{ckg4j_root}/{args.output_dir}',
        damping_factor=args.damping_factor,
        epsilon=args.epsilon,
        max_iterations=args.max_iterations,
        overwrite=args.overwrite,
    )