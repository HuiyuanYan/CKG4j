import decimal
from collections import defaultdict


class DirectedGraph:
    def __init__(self):
        self.adjacency_list = defaultdict(dict)

    def add_node(self, node):
        self.adjacency_list[node]

    def add_edge(self, source, destination, weight):
        self.add_node(source)
        self.add_node(destination)
        self.adjacency_list[source][destination] = weight

    def get_nodes(self):
        return self.adjacency_list.keys()

    def get_edge_weight(self, node1, node2):
        return self.adjacency_list.get(node1, {}).get(node2, 0.0)

    def remove_edge(self, source, destination):
        self.adjacency_list[source].pop(destination, None)

    def get_neighbors(self, node):
        return self.adjacency_list.get(node, {})

    def has_node(self, node):
        return node in self.adjacency_list

    def normalize_weights(self):
        for node, neighbors in self.adjacency_list.items():
            weight_sum = sum(neighbors.values())
            if weight_sum == 0:
                continue
            for neighbor in neighbors:
                weight = neighbors[neighbor]
                normalized_weight = weight / weight_sum
                # Format to 5 decimal places
                neighbors[neighbor] = round(decimal.Decimal(normalized_weight), 5)

    def __str__(self):
        result = []
        for node, neighbors in sorted(self.adjacency_list.items()):
            neighbor_str = "|".join(
                f"{neighbor}@{weight}" for neighbor, weight in sorted(neighbors.items())
            )
            result.append(f"{node}\t[{neighbor_str}]")
        return "\n".join(result)
