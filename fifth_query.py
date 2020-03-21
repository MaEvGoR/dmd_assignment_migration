from second_query import query_two
import networkx as nx

def query_five(mongo_client):
    weights = query_two(mongo_client)
    actor1 = weights[0][0]

    answer = []
    G = nx.Graph()

    nodes = []
    for i in range(len(weights)):
        nodes.append(weights[i][0])
        nodes.append(weights[i][1])

    G.add_nodes_from(list(set(nodes)))

    for i in range(len(weights)):
        if weights[i][2] != 0:
            G.add_edge(weights[i][0], weights[i][1])

    for person in nodes:
        answer.append([actor1, person, nx.shortest_path_length(G, source=actor1, target=person)])

    return answer
