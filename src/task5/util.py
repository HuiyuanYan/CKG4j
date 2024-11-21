from .directed_graph import DirectedGraph
from .undirected_graph import UndirectedGraph

def convert_to_undirected_graph(directed_graph):
    undirected_graph = UndirectedGraph()

    for node in directed_graph.get_nodes():
        undirected_graph.add_node(node)
        for neighbor, weight in directed_graph.get_neighbors(node).items():
            undirected_graph.add_edge(node, neighbor, 0.0)

    max_degree = undirected_graph.get_max_degree()

    for node in undirected_graph.get_nodes():
        for neighbor in undirected_graph.get_neighbors(node):
            new_weight = (
                directed_graph.get_edge_weight(node, neighbor)
                * (undirected_graph.get_degree(node) / max_degree)
                + directed_graph.get_edge_weight(neighbor, node)
                * (undirected_graph.get_degree(neighbor) / max_degree)
            )
            undirected_graph.adjacency_list[node][neighbor] = new_weight

    return undirected_graph
