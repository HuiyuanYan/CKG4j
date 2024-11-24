import os
import sys
import traceback
sys.path.append(os.getcwd())
import subprocess
import argparse
import json
from src.settings import Settings
from src.util import (
    delete_hdfs_path,
    create_hdfs_path,
    upload_to_hdfs,
    download_hdfs_file,
    build_logger,
)
from src.task0.entity_filter import filter_entities

logger = build_logger()

hadoop_streaming_path = Settings.basic_settings.hadoop_streaming_path
ckg4j_root = Settings.CKG4J_ROOT

def save_to_json(json_path, data):
    """
    将数据保存为 JSON 文件。
    """
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def read_statistics(file_or_dir):
    """
    读取 reducer 输出结果，格式为：名称\t频次。
    """
    data = []
    if os.path.isdir(file_or_dir):
        files = [os.path.join(file_or_dir, f) for f in os.listdir(file_or_dir)]
    else:
        files = [file_or_dir]

    for file in files:
        if not os.path.isfile(file):
            continue
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                name, count = line.split("\t")
                data.append((name, int(count)))
    return data


def run(
    input_dir: str,
    output_dir: str,
    json_output_path: str,
    extract_method: str = "jieba",
    entity_type: str = "nr",
    spacy_model_name: str = "zh_core_web_sm",
    overwrite: bool = False,
    top_n: int = None,
    min_freq: int = None,
    use_llm: bool = False,
):
    if extract_method == "jieba" and entity_type == "PERSON":
        logger.info("When using jieba, PERSON will be mapped to 'nr")
        entity_type = "nr"

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

    # 构建 Hadoop 命令
    hadoop_cmd = f"""
hadoop jar {hadoop_streaming_path} \
    -D stream.num.map.output.key.fields=1 \
    -input {input_dir} \
    -output {output_dir} \
    -mapper 'python {ckg4j_root}/src/task0/mapper.py {extract_method} {entity_type} {spacy_model_name}' \
    -reducer 'python {ckg4j_root}/src/task0/reducer.py' \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner
"""

    # 运行 Hadoop 命令
    try:
        subprocess.run(hadoop_cmd, shell=True, check=True)
    except Exception as e:
        logger.error(f"Error running hadoop command: {hadoop_cmd}")
        logger.error(str(e))
    
    try:
        # 下载输出文件夹到本地
        os.makedirs(output_dir, exist_ok=True)
        download_hdfs_file(output_dir, output_dir)

        # 读取初筛结果
        data = read_statistics(output_dir)[0:100]

        # 根据选择的筛选方法处理数据
        logger.info("Filtering entities...")
        final_character_dict = filter_entities(
            data, 
            top_n=top_n, 
            min_frequency=min_freq, 
            use_llm=use_llm
        )

        # 保存为 JSON
        if json_output_path:
            save_to_json(json_output_path, final_character_dict)
            upload_to_hdfs(json_output_path, json_output_path)
            logger.info(f"Character JSON generated successfully: {json_output_path}")

        logger.info(f"Task0 finished successfully, output_dir: {output_dir}")
    except Exception as e:
        print(traceback.format_exc())
        logger.error(f"Error occurred during processing: {str(e)}")


if __name__ == "__main__":
    logger.info(f"Start running task0, ckg4j_root: {ckg4j_root}, hadoop_streaming_path: {hadoop_streaming_path}")
    parser = argparse.ArgumentParser(description="Run Hadoop Task0 and optionally refine results.")
    parser.add_argument("--input_dir", help="Path to the input files directory")
    parser.add_argument("--output_dir", default="task0/output", help="HDFS output dir")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files on HDFS")
    parser.add_argument("--extractor_method", type=str, choices=["jieba", "nltk", "spacy"], default="jieba",
                        help="Extractor method")
    parser.add_argument("--entity_type", type=str, choices=["PERSON", "nr"], default="PERSON", help="Entity type")
    parser.add_argument("--spacy_model_name", type=str, default="zh_core_web_sm", help="Spacy model name")
    parser.add_argument("--json_output_path", type=str, default=None, help="Path to save character.json")
    parser.add_argument("--top_n", type=int, default=None, help="Select top N names by frequency (optional).")
    parser.add_argument("--min_freq", type=int, default=None,
                        help="Select names with frequency >= min_freq (optional).")
    parser.add_argument("--use_llm", action="store_true", help="Use LLM to filter and format results.")

    args = parser.parse_args()
    logger.info(f"Task0 args: {args._get_args()}")

    run(
        f"{ckg4j_root}/{args.input_dir}",
        f"{ckg4j_root}/{args.output_dir}",
        f"{ckg4j_root}/{args.json_output_path}",
        extract_method=args.extractor_method,
        entity_type=args.entity_type,
        spacy_model_name=args.spacy_model_name,
        overwrite=args.overwrite,
        top_n=args.top_n,
        min_freq=args.min_freq,
        use_llm=args.use_llm,
    )
