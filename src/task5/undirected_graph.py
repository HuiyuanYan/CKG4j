from collections import defaultdict
from .union_find import UnionFind

class UndirectedEdge:
    def __init__(self, node1, node2, weight):
        self.node1 = node1
        self.node2 = node2
        self.weight = weight

    def __eq__(self, other):
        if not isinstance(other, UndirectedEdge):
            return False
        return {self.node1, self.node2} == {other.node1, other.node2}

    def __hash__(self):
        return hash(frozenset({self.node1, self.node2}))


class UndirectedGraph:
    def __init__(self):
        self.adjacency_list = defaultdict(dict)

    def add_node(self, node):
        self.adjacency_list[node]

    def add_edge(self, node1, node2, weight):
        self.add_node(node1)
        self.add_node(node2)
        self.adjacency_list[node1][node2] = weight
        self.adjacency_list[node2][node1] = weight

    def get_neighbors(self, node):
        return self.adjacency_list.get(node, {})

    def get_degree(self, node):
        return len(self.adjacency_list[node])

    def get_nodes(self):
        return self.adjacency_list.keys()

    def set_edge_weight(self, node1, node2, weight):
        if node1 in self.adjacency_list and node2 in self.adjacency_list:
            self.adjacency_list[node1][node2] = weight
            self.adjacency_list[node2][node1] = weight

    def get_edges(self):
        edges = set()
        for node1, neighbors in self.adjacency_list.items():
            for node2, weight in neighbors.items():
                edges.add(UndirectedEdge(node1, node2, weight))
        return list(edges)

    def get_max_degree(self):
        return max(len(neighbors) for neighbors in self.adjacency_list.values())

    def get_minimum_spanning_tree(self):
        mst = UndirectedGraph()
        edges = sorted(self.get_edges(), key=lambda edge: edge.weight)
        union_find = UnionFind(len(self.get_nodes()))
        nodes = list(self.adjacency_list.keys())
        node_to_index = {node: i for i, node in enumerate(nodes)}

        for edge in edges:
            idx1 = node_to_index[edge.node1]
            idx2 = node_to_index[edge.node2]
            if union_find.find(idx1) != union_find.find(idx2):
                union_find.union(idx1, idx2)
                mst.add_edge(edge.node1, edge.node2, edge.weight)
        return mst

    def __str__(self):
        result = []
        for node, neighbors in sorted(self.adjacency_list.items()):
            neighbor_str = "|".join(
                f"{neighbor}@{weight}" for neighbor, weight in sorted(neighbors.items())
            )
            result.append(f"{node}\t[{neighbor_str}]")
        return "\n".join(result)
