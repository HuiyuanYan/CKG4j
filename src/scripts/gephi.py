import pandas as pd

# 定义文件路径
input_file = 'task6/graph_0.txt'
nodes_output = 'nodes.csv'
edges_output = 'edges.csv'

# 准备存储节点和边的列表
nodes = []
edges = []

# 分组 ID 计数器
group_counter = 1

# 读取文件并解析
with open(input_file, 'r', encoding='utf-8') as f:
    lines = f.readlines()

for line in lines:
    # 分离节点部分和关系部分
    node_part, connections = line.strip().split('#', 1)
    connections, size = connections.rsplit('#', 1)
    size = float(size)

    # 添加到节点列表
    nodes.append({'Id': node_part, 'Label': node_part, 'Size': size, 'Group': f'Group{group_counter}'})

    # 添加到边列表
    for connection in connections.split('|'):
        target, weight = connection.split('@')
        weight = float(weight)
        edges.append({'Source': node_part, 'Target': target, 'Weight': weight, 'Type': 'Undirected'})

    group_counter += 1

# 转换为 DataFrame
nodes_df = pd.DataFrame(nodes)
edges_df = pd.DataFrame(edges)

# 添加 Group 列，基于 Source 字段
edges_df['Group'] = edges_df['Source'].apply(lambda x: f'Group_{x}')

# 保存为 CSV 文件
nodes_df.to_csv(nodes_output, index=False)
edges_df.to_csv(edges_output, index=False)

print(f"Nodes file saved to: {nodes_output}")
print(f"Edges file saved to: {edges_output}")
