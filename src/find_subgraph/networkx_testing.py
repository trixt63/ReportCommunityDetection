import networkx as nx


def main():
    g = nx.Graph()

    g.add_edge('a0', 'a1', time=1)
    g.add_edge('a1', 'a3', time=2)
    g.add_edge('a3', 'a2', time=3)
    g.add_edge('a2', 'a6', time=1)
    g.add_edge('a3', 'a4', time=1)
    g.add_edge('a5', 'a4', time=1)
    print(g.get_edge_data('a0', 'a1').get('time'))


if __name__ == '__main__':
    main()
