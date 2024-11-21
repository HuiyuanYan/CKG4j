class DirectedWeightedGraph:
    def __init__(self):
        self.adjacency_map = {}  # 存储节点与其出边邻居及对应权重

    def add_node(self, node):
        if node not in self.adjacency_map:
            self.adjacency_map[node] = {}

    def add_edge(self, from_node, to_node, weight):
        if from_node not in self.adjacency_map:
            self.add_node(from_node)
        if to_node not in self.adjacency_map:
            self.add_node(to_node)
        self.adjacency_map[from_node][to_node] = weight

    def get_neighbors(self, node):
        return self.adjacency_map.get(node, {})

    def get_nodes(self):
        return self.adjacency_map.keys()

    def print_graph(self):
        for node, neighbors in self.adjacency_map.items():
            neighbors_str = ", ".join(f"{neighbor}@{weight}" for neighbor, weight in neighbors.items())
            print(f"Node: {node}, Neighbors: {neighbors_str}")

    def get_k_value_of_nodes(self, k):
        """
        K-core 分析，计算每个节点的 k 值。
        """
        adjacency_map_copy = {node: dict(neighbors) for node, neighbors in self.adjacency_map.items()}
        node_k_value_map = {node: k for node in adjacency_map_copy}

        for current_k in range(1, k):
            if not adjacency_map_copy:
                break
            nodes_to_remove = set()
            has_degree_k_nodes = True
            while has_degree_k_nodes:
                has_degree_k_nodes = False
                for node, neighbors in adjacency_map_copy.items():
                    if len(neighbors) <= current_k and node not in nodes_to_remove:
                        node_k_value_map[node] = current_k
                        nodes_to_remove.add(node)
                        has_degree_k_nodes = True
            for node in nodes_to_remove:
                for neighbor in list(adjacency_map_copy.keys()):
                    if node in adjacency_map_copy[neighbor]:
                        del adjacency_map_copy[neighbor][node]
                del adjacency_map_copy[node]
        return node_k_value_map

    def get_normalized_degree_values(self):
        """
        归一化节点出度。
        """
        normalized_degree_values = {}
        max_degree = max(len(neighbors) for neighbors in self.adjacency_map.values()) if self.adjacency_map else 1
        for node, neighbors in self.adjacency_map.items():
            normalized_degree = len(neighbors) / max_degree
            normalized_degree_values[node] = normalized_degree
        return normalized_degree_values

    def get_average_degree(self):
        """
        计算图的平均出度。
        """
        total_degree = sum(len(neighbors) for neighbors in self.adjacency_map.values())
        num_nodes = len(self.adjacency_map)
        return total_degree // num_nodes if num_nodes else 0
