import os
import sys
import shutil
import traceback
import argparse
import subprocess
sys.path.append(os.getcwd())
from src.task6.directed_weighted_graph import DirectedWeightedGraph
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



def read_graph(graph_info_dir, main_characters=None, top_k_characters=5):
    """
    读取图信息，返回 `graph_info` 和 `label_info`。
    """
    def get_initial_communities(node_influence, top_k=5):
        sorted_nodes = sorted(node_influence.items(), key=lambda item: item[1], reverse=True)[:top_k]
        return [node for node, _ in sorted_nodes]

    graph = DirectedWeightedGraph()
    for filename in os.listdir(graph_info_dir):
        file_path = os.path.join(graph_info_dir, filename)
        if os.path.isfile(file_path):
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    parts = line.strip().split("\t")
                    if len(parts) != 2:
                        continue

                    node = parts[0]
                    neighbors_data = parts[1].strip("[]").split("|")

                    graph.add_node(node)
                    for neighbor_data in neighbors_data:
                        if "@" in neighbor_data:
                            neighbor_node, weight = neighbor_data.split("@")
                            graph.add_edge(node, neighbor_node, float(weight))


    # 计算 k 值、归一化度数和影响力
    k = graph.get_average_degree()
    node_k_value = graph.get_k_value_of_nodes(k)
    node_normalized_degree = graph.get_normalized_degree_values()
    node_influence = {node: node_k_value[node] + node_normalized_degree[node] for node in node_k_value}

    # 构造图信息
    graph_info = []
    for node, influence in node_influence.items():
        neighbors = '|'.join([f"{neighbor}@{weight}" for neighbor, weight in graph.get_neighbors(node).items()])
        graph_info.append(f"{node}#{neighbors}#{influence:.4f}")

    k_min = min(node_k_value.values())

    # 确定初始社区
    if not main_characters:
        initial_communities = get_initial_communities(node_influence, top_k_characters)
    else:
        initial_communities = main_characters

    logger.info(f"Initial communities: {initial_communities}")
    assert initial_communities, "No main characters found."

    # 构造标签信息
    label_info = []
    for node, k_value in node_k_value.items():
        if k_value == k_min:
            label_info.append(f"{node}#Y#N")  # 边缘层节点
        elif node in initial_communities:
            label_info.append(f"{node}#N#{node}")  # 初始社区节点
        else:
            label_info.append(f"{node}#N#N")  # 非边缘层节点，无标签

    return graph_info, label_info


def run(
    page_rank_results_dir:str,
    graph_info_dir:str,
    output_dir:str,
    top_k_characters:int=5,
    max_iterations:int=50,
    main_character_source:str='page_rank',
    overwrite:bool=False,
    save_temp: bool = False,
):
    # 获取初始社区的主要角色
    if main_character_source == 'page_rank':
        # 读取page rank结果并获取前top_k_character个角色
        characters_with_rank = []
        for filename in os.listdir(page_rank_results_dir):
            file_path = os.path.join(page_rank_results_dir, filename)
            with open(file_path, 'r') as file:
                for line in file:
                    parts = line.strip().replace('\t','').split("|")
                    if len(parts) == 2:
                        character, rank = parts
                        characters_with_rank.append((character, float(rank)))
        
        # 按page rank分数从高到低排序
        characters_with_rank.sort(key=lambda x: x[1], reverse=True)
        
        # 获取前top_k_character个角色
        main_characters = [character for character, _ in characters_with_rank[:top_k_characters]]
        pass
    elif main_character_source == 'node_influence':
        main_character_source = [] # 读取图信息时根据节点影响力自动生成
    
    logger.info("Reading graph info...")
    graph_info, label_info = read_graph(graph_info_dir, main_characters, top_k_characters)
    graph_info.sort()
    label_info.sort()
    print("Graph info:", graph_info)
    print("Label info:", label_info)
    
    iteration = 0
    # 将graph_info和label_info写入本地文件并上传至hdfs
    os.makedirs(output_dir,exist_ok=True)
    graph_info_path = f'{output_dir}/graph_{iteration}'
    label_info_path = f'{output_dir}/label_{iteration}'

    delete_hdfs_path(output_dir)
    
    shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    with open(graph_info_path,'w') as f:
        for info in graph_info:
            line = f'{info}\n'
            f.write(line)
    
    with open(label_info_path,'w') as f:
        for info in label_info:
            line = f'{info}\n'
            f.write(line)

    upload_to_hdfs(graph_info_path, graph_info_path,True)
    upload_to_hdfs(label_info_path, label_info_path,True)

    updated = True
    handle_edge = False
    while True:
        if not updated:
            if handle_edge:
                logger.info("Non edge nodes have been updated, the edge nodes will be updated...")
            else:
                logger.info("All nodes have been updated, finish.")
                break
        if iteration >= max_iterations:
            logger.info("Max iterations reached, finish.")
            break

        input_label_info_path = f'{output_dir}/label_{iteration}'
        output_label_info_path = f'{output_dir}/label_{iteration+1}'
        # 先处理非边缘节点
        hadoop_cmd = f"""
hadoop jar {hadoop_streaming_path} \
    -input {input_label_info_path} \
    -output {output_label_info_path}\
    -mapper 'python src/task6/mapper.py {input_label_info_path} {graph_info_path} {handle_edge}'\
    -reducer 'python src/task6/reducer.py'
"""
        try:
            logger.info(f"Running iteration {iteration}, handle_edge = {handle_edge}")
            result = subprocess.run(
                hadoop_cmd,
                shell=True,
                check=True,
                stderr=subprocess.PIPE,
                text=True
            )
            print(result.stderr)
            if 'task6 reporter: Nodes have been updated' in result.stderr:
                updated = True
                if handle_edge: # 边缘节点只处理一轮，统一更新
                    handle_edge = False
                    updated = False
            else:
                updated = False
                if not handle_edge:
                    handle_edge = True
            
            download_hdfs_file(output_label_info_path,output_label_info_path)
            logger.info(f'Iteration {iteration} finished, output saved to {output_label_info_path}')
            iteration += 1
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            logger.error(f"Error running hadoop command: {hadoop_cmd}")
            return
    
    if not save_temp:
        for i in range(0,iteration):
            temp_path = f'{output_dir}/label_{i}'
            # 如果是文件
            if os.path.isfile(temp_path):
                os.remove(temp_path)
            else:
                shutil.rmtree(temp_path)
            # 先删除本地文件，再删除hdfs文件
            delete_hdfs_path(temp_path)
        logger.info("Temporary files deleted")
    pass


if __name__ == "__main__":
    logger.info(f"Start running task6, ckg4j_root: {ckg4j_root}, hadoop_streaming_path: {hadoop_streaming_path}")
    parser = argparse.ArgumentParser()
    parser.add_argument("--page_rank_results_dir",default="",type=str)
    parser.add_argument("--graph_info_dir", type=str)
    parser.add_argument("--output_dir", type=str)
    parser.add_argument("--top_k_character",type=int,default=5)
    parser.add_argument("--max_iteration", type=int, default=20)
    parser.add_argument("--main_character_source", choices=['page_rank','node_influence'],type=str, default='page_rank')
    parser.add_argument("--overwrite", type=bool, default=False)
    parser.add_argument("--save_temp", type=bool, default=False, help="Whether to save intermediate results of label propagation")
    args = parser.parse_args()
    logger.info(f"Task6 args: {args._get_args()}")
    
    run(
        page_rank_results_dir=f'{ckg4j_root}/{args.page_rank_results_dir}',
        graph_info_dir=f'{ckg4j_root}/{args.graph_info_dir}',
        output_dir=f'{ckg4j_root}/{args.output_dir}',
        top_k_characters=args.top_k_character,
        max_iterations=args.max_iteration,
        main_character_source=args.main_character_source,
        overwrite=args.overwrite,
        save_temp=args.save_temp
    )