class BellmanFord(object):

    def __init__(self, initial_edges=None):
        self.vertices = set()
        self.edges = {}
        if initial_edges is not None:
            for u in initial_edges:
                for v in initial_edges[u]:
                    self.add_edge(u, v, initial_edges[u][v])
    

    def add_edge(self, from_vertex, to_vertex, weight):
        if from_vertex == to_vertex:
            raise ValueError('{}->{}'.format(from_vertex, to_vertex))
        self.vertices.add(from_vertex)
        self.vertices.add(to_vertex)
        if from_vertex not in self.edges:
            self.edges[from_vertex] = {}
        self.edges[from_vertex][to_vertex] = weight
    
    def remove_edge(self, from_vertex, to_vertex):
        try:
            del self.edges[from_vertex][to_vertex]
        except KeyError:
            raise KeyError('remove_edge {}->{}'.format(from_vertex, to_vertex))
    
    def shortest_path(self, start_vertex, tolerance=0):
        """
        Find the shortest path from start vertex to every other vertex
        also detect if there are negative cycles and report one of them.
        """
        distance, predecessor = {}, {}
        for v in self.vertices:
            distance[v] = float('inf')
            predecessor[v] = None
        distance[start_vertex] = 0

        for i in range(len(self.vertices)):
            for u in self.edges:
                for v in self.edges[u]:
                    w = self.edges[u][v]
                    if distance[v] - (distance[u] + w) > tolerance:
                        if v == start_vertex:
                            return distance, predecessor, (u, v)
                        distance[v] = distance[u] + w
                        predecessor[v] = u
        
        for u in self.edges:
            for v in self.edges[u]:
                w = self.edges[u][v]
                if distance[v] - (distance[u] + w) > tolerance:
                    return distance, predecessor, (u, v)
        
        return distance, predecessor, None
