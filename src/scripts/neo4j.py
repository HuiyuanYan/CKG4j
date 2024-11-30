from neo4j import GraphDatabase
import pandas as pd

# Neo4j 数据库连接信息
NEO4J_URI = "neo4j://localhost:7687"  # 修改为你的 Neo4j 地址
NEO4J_USER = "neo4j"  # 用户名
NEO4J_PASSWORD = "12345678"  # 密码
# 文件路径
community_file_path = r"D:\code\DataBase\CKG4j-main2\CKG4j-main\data\hp\output\task6\label_5\part-00000"
adjacency_file_path = r"D:\code\DataBase\CKG4j-main2\CKG4j-main\data\hp\output\task6\graph_0"


# 定义 Neo4j 导入类
class Neo4jImporter:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def import_community_data(self, community_data):
        with self.driver.session() as session:
            for _, row in community_data.iterrows():
                # 清理 community 值，移除空格或特殊字符
                sanitized_community = row['community'].replace(" ", "_").replace("-", "_")
                session.run(
                    f"""
                    MERGE (p:{sanitized_community} {{name: $name}})
                    SET p.isEdgeNode = $is_edge_node
                    """,
                    name=row['name'],
                    is_edge_node=row['is_edge_node'],
                )

    def import_adjacency_data(self, adjacency_data):
        with self.driver.session() as session:
            for _, row in adjacency_data.iterrows():
                session.run(
                    """
                    MERGE (p1 {name: $person})
                    WITH p1
                    UNWIND $neighbors AS neighbor
                    MERGE (p2 {name: neighbor.name})
                    MERGE (p1)-[:CONNECTED]->(p2)
                    """,
                    person=row['person'],
                    neighbors=row['neighbors'],
                )

# 解析社群关系文件
def parse_community_file(file_path):
    community_data = []
    # 修改：显式指定编码为 'utf-8' 或其他正确的编码
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split('#')
            community_data.append({
                'name': parts[0],
                'is_edge_node': parts[1] == 'True',
                'community': parts[2],
            })
    return pd.DataFrame(community_data)

# 解析邻接关系文件
def parse_adjacency_file(file_path):
    adjacency_data = []
    # 修改：显式指定编码为 'utf-8' 或其他正确的编码
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split('#')
            neighbors = []
            if len(parts) > 1 and parts[1]:
                for neighbor in parts[1].split('|'):
                    neighbor_parts = neighbor.split('@')
                    neighbors.append({
                        'name': neighbor_parts[0],
                        'weight': float(neighbor_parts[1]),  # 权重仍然解析，但不会导入到关系中
                    })
            adjacency_data.append({
                'person': parts[0],
                'neighbors': neighbors,
            })
    return pd.DataFrame(adjacency_data)

# 主程序
if __name__ == "__main__":
    # 解析文件
    try:
        # 解析社区文件
        community_data = parse_community_file(community_file_path)
        # 解析邻接文件
        adjacency_data = parse_adjacency_file(adjacency_file_path)
    except UnicodeDecodeError as e:
        print(f"文件编码错误：{e}")
        print("请确认文件的实际编码格式，并修改代码中的 'encoding' 参数！")
        exit(1)

    # 初始化 Neo4j 导入器
    importer = Neo4jImporter(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

    try:
        # 清空数据库中的现有节点和关系
        with importer.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("数据库已清空！")

        # 导入数据到 Neo4j
        importer.import_community_data(community_data)
        importer.import_adjacency_data(adjacency_data)
        print("数据成功导入到 Neo4j！")
    finally:
        importer.close()
