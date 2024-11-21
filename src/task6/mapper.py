import sys
import os
from collections import defaultdict

import os

def read_file_or_directory(path):
    """
    读取给定路径下的内容。
    如果是文件，则读取该文件的内容。
    如果是文件夹，则读取该文件夹下所有一级子文件的内容。
    返回按行分割的内容列表。
    """
    data = []

    if os.path.isfile(path):
        # 如果是文件，直接读取文件内容
        with open(path, 'r', encoding='utf-8') as f:
            data.extend(f.readlines())
    elif os.path.isdir(path):
        # 如果是文件夹，读取文件夹下所有一级子文件内容
        for root, _, files in os.walk(path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                if os.path.isfile(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data.extend(f.readlines())
            break  # 仅处理一级子文件
    else:
        raise ValueError(f"Path '{path}' is neither a file nor a directory.")

    return [line.strip() for line in data]


def read_graph_config(graph_file_path):
    """
    从文件夹读取图配置信息，返回解析后的字典结构：
    - node_neighbors: {节点: {邻居1: 权重, 邻居2: 权重, ...}}
    - node_influence: {节点: 节点影响值}
    """
    node_neighbors = defaultdict(dict)
    node_influence = {}
    lines = read_file_or_directory(graph_file_path)
    for line in lines:
        parts = line.split("#")
        if len(parts) == 3:
            node, neighbor_weight, influence = parts
            node_influence[node] = float(influence)
            for neighbor_data in neighbor_weight.split("|"):
                if "@" in neighbor_data:
                    neighbor, weight = neighbor_data.split("@")
                    node_neighbors[node][neighbor] = float(weight)
    return node_neighbors, node_influence

def read_label_config(label_info_path):
    """
    从文件或目录加载初始的标签信息。
    返回字典结构：{节点: 标签}
    """
    label_info = {}
    lines = read_file_or_directory(label_info_path)
    for line in lines:
        parts = line.split("#")
        if len(parts) == 3:
            node,is_edge,label = parts
            label_info[node] = label
    return label_info

def update_label(node, is_edge, old_label, node_neighbors, node_influence, label_info, handle_edge):
    """
    根据图信息和原始标签更新节点标签。
    """
    if (handle_edge and is_edge == "N") or (not handle_edge and is_edge == "Y"):
        return old_label
    label_influence = defaultdict(float)

    # 自身标签影响
    if old_label != "N":
        label_influence[old_label] += node_influence.get(node, 0)

    # 处理邻居影响
    for neighbor, weight in node_neighbors.get(node, {}).items():
        neighbor_label = label_info.get(neighbor, "N")
        if neighbor_label != "N":
            influence = node_influence.get(neighbor, 0) * weight
            label_influence[neighbor_label] += influence

    # 获取影响值最大的标签
    if not label_influence:
        return old_label

    # 如果多个标签的影响值相同，按字典序选取
    new_label = max(sorted(label_influence.keys()), key=lambda label: label_influence[label])
    #print(f'node: {node} old_label:{old_label} influence: {label_influence}, new_label: {new_label}')
    return new_label

def process(graph_file_path, handle_edge):
    """
    主处理函数，从sys.stdin读取节点信息并更新标签。
    """
    # 读取图配置信息
    node_neighbors, node_influence = read_graph_config(graph_file_path)

    # 加载初始标签信息
    label_info = read_label_config(label_info_path)
    #print(f'node_neighbors: {node_neighbors}')
    #print(f'node_influence: {node_influence}')
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        # 将单词按 '#' 划分：node, is_edge, old_label
        parts = line.split("#")
        if len(parts) != 3:
            continue

        node, is_edge, old_label = parts

        # 更新标签
        new_label = update_label(
            node, is_edge, old_label,
            node_neighbors, node_influence, label_info, handle_edge
        )

        if old_label != new_label:
            sys.stderr.write(f'task6 reporter: Nodes have been updated')

        # 输出更新后的标签信息
        print(f"{node}#{is_edge}#{new_label}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python mapper.py <label_info_path> <graph_info_path> <handleEdge>")
        sys.exit(1)

    label_info_path = sys.argv[1]
    graph_info_path = sys.argv[2]
    handle_edge = sys.argv[3].lower() == 'true'
    process(graph_info_path, handle_edge)
