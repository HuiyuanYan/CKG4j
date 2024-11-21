import os
import sys
sys.path.append(os.getcwd())
import subprocess
import argparse
from src.settings import Settings
from src.util import (
    delete_hdfs_path,
    create_hdfs_path,
    upload_to_hdfs,
    build_logger,
)

logger = build_logger()
ckg4j_root = Settings.CKG4J_ROOT



def run(input_dir: str, output_dir: str, alpha: float, overwrite: bool):
    """Run Task5 filtering and handle HDFS interactions."""
    create_hdfs_path(input_dir)

    # 上传输入文件
    if os.path.isdir(input_dir):
        for file in os.listdir(input_dir):
            file_path = f"{input_dir}/{file}"
            if os.path.isfile(file_path):
                upload_to_hdfs(file_path, file_path, overwrite)
    elif os.path.isfile(input_dir):
        upload_to_hdfs(input_dir, input_dir, overwrite)

    delete_hdfs_path(output_dir)



    # 本地处理
    output_path = f"{output_dir}/output"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    try:
        relationship_filter_script = f"{ckg4j_root}/src/task5/relationship_filter.py"
        cmd = (
            f"python {relationship_filter_script} "
            f"--input_dir {input_dir} "
            f"--output_file {output_path} "
            f"--alpha {alpha}"
        )
        subprocess.run(cmd, shell=True, check=True)
        upload_to_hdfs(output_path, output_dir, overwrite)
        logger.info(f"Task5 finished successfully, output_dir: {output_dir}")
    except:
        logger.error(f"Error running relationship_filter.py: {cmd}")
        return
    

if __name__ == "__main__":
    logger.info(f"Start running task5, ckg4j_root: {ckg4j_root}")
    parser = argparse.ArgumentParser(description="Run Task5 with HDFS interaction.")
    parser.add_argument("--input_dir", help="HDFS path to the input files directory")
    parser.add_argument("--output_dir", help="HDFS output directory")
    parser.add_argument("--alpha", type=float, default=0.8, help="Proportion of edges to remove (default: 0.8)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files on HDFS")

    args = parser.parse_args()
    logger.info(f"Task5 args: {args._get_args()}")

    run(
        input_dir=f"{ckg4j_root}/{args.input_dir}",
        output_dir=f"{ckg4j_root}/{args.output_dir}",
        alpha=args.alpha,
        overwrite=args.overwrite,
    )
