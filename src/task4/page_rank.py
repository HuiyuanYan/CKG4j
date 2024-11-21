import os
from collections import defaultdict
import argparse

def read_input(directory):
    graph = defaultdict(lambda: defaultdict(float))
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split("\t")
                    if len(parts) != 2:
                        continue
                    main_character, related_characters = parts
                    if not related_characters:
                        continue
                    related_characters = related_characters.strip("[]").split("|")
                    for related_character in related_characters:
                        if related_character:
                            character, frequency = related_character.split("@")
                            graph[main_character][character] += float(frequency)
    return graph

def pagerank_iteration(graph, damping_factor, epsilon, max_iterations):
    num_nodes = len(graph)
    if num_nodes == 0:
        return {}
    pagerank = {node: 1.0 / num_nodes for node in graph}
    for _ in range(max_iterations):  # 设定一个最大迭代次数
        new_pagerank = {node: (1 - damping_factor) / num_nodes for node in graph}
        for node in graph:
            for neighbor, frequency in graph[node].items():
                if neighbor in new_pagerank:  # 确保邻居节点存在于图中
                    new_pagerank[neighbor] += damping_factor * (pagerank[node] / sum(graph[node].values()) * frequency)
        delta = 0
        for node in graph:
            delta = max(delta, abs(new_pagerank[node] - pagerank[node]))
        pagerank = new_pagerank
        if delta < epsilon:
            break
    return pagerank

def main(input_directory, output_directory, damping_factor, epsilon, max_iterations):
    graph = read_input(input_directory)
    pagerank = pagerank_iteration(graph, damping_factor, epsilon, max_iterations)
    
    # Ensure output directory exists
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    
    output_filename = os.path.join(output_directory, 'pagerank_results')
    with open(output_filename, 'w', encoding='utf-8') as f:
        sorted_pagerank = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)
        for node, score in sorted_pagerank:
            f.write(f"{node}|{score}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PageRank Algorithm")
    parser.add_argument("--input_dir", type=str, help="Directory containing input files")
    parser.add_argument("--output_dir", type=str, help="Directory to save output file")
    parser.add_argument("--damping_factor", type=float, default=0.85, help="Damping factor for PageRank (default: 0.85)")
    parser.add_argument("--epsilon", type=float, default=1.0e-6, help="Epsilon for convergence (default: 1.0e-6)")
    parser.add_argument("--max_iterations", type=int, default=100, help="Maximum number of iterations (default: 100)")

    args = parser.parse_args()
    main(args.input_dir, args.output_dir, args.damping_factor, args.epsilon, args.max_iterations)