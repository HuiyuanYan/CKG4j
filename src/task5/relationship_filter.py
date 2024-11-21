import sys,os
sys.path.append(os.getcwd())
import argparse
from copy import deepcopy
from pathlib import Path
from src.task5.directed_graph import DirectedGraph
from src.task5.undirected_graph import UndirectedGraph, UndirectedEdge

def read_graph(file_path: str) -> DirectedGraph:
    """Read a directed graph from input files."""
    graph = DirectedGraph()
    input_path = Path(file_path)

    if input_path.is_dir():
        for file in input_path.iterdir():
            with file.open("r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split("\t")
                    node = parts[0]
                    neighbor_weight_str = parts[1][1:-1]  # Remove brackets
                    graph.add_node(node)
                    for pair in neighbor_weight_str.split("|"):
                        neighbor, weight = pair.split("@")
                        graph.add_edge(node, neighbor, float(weight))
    else:
        raise FileNotFoundError(f"Directory {file_path} does not exist.")
    return graph


def convert_to_undirected_graph(directed_graph: DirectedGraph) -> UndirectedGraph:
    """Convert directed graph to an undirected graph."""
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


def filter_edges(alpha: float, directed_graph: DirectedGraph, edges: list[UndirectedEdge]) -> DirectedGraph:
    """Filter edges based on the alpha parameter."""
    graph = deepcopy(directed_graph)
    edges_to_remove = []

    for node in directed_graph.get_nodes():
        neighbors = directed_graph.get_neighbors(node)
        sorted_neighbors = sorted(neighbors.items(), key=lambda x: x[1])
        delete_edge_count = int(len(neighbors) * alpha)

        for i, (neighbor, weight) in enumerate(sorted_neighbors):
            if i < delete_edge_count and UndirectedEdge(node, neighbor, weight) not in edges:
                edges_to_remove.append((node, neighbor))

    for node1, node2 in edges_to_remove:
        graph.remove_edge(node1, node2)
        graph.remove_edge(node2, node1)

    graph.normalize_weights()
    return graph


def save_graph(output_path: str, graph: DirectedGraph):
    """Save graph to the specified output path."""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(str(graph))


def main():
    parser = argparse.ArgumentParser(description="Filter edges from a directed graph.")
    parser.add_argument("--input_dir", type=str, help="Path to the directory containing input graph files.")
    parser.add_argument("--output_file", type=str, help="Path to the output file to save the filtered graph.")
    parser.add_argument("--alpha", type=float, default=0.8, help="Proportion of edges to remove (default: 0.8).")
    args = parser.parse_args()

    input_dir = args.input_dir
    output_file = args.output_file
    alpha = args.alpha

    graph1 = read_graph(input_dir)
    graph2 = convert_to_undirected_graph(graph1)
    minimum_spanning_tree = graph2.get_minimum_spanning_tree()
    new_graph = filter_edges(alpha, graph1, minimum_spanning_tree.get_edges())

    save_graph(output_file, new_graph)


if __name__ == "__main__":
    main()
