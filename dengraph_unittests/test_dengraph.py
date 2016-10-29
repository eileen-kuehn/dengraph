import unittest
import random
import csv
import os
import zipfile
import sys
import io

from dengraph.dengraph import DenGraphIO
from dengraph.graphs.distance_graph import DistanceGraph

import dengraph_unittests


class DeltaDistance(object):
    is_symmetric = True

    def __call__(self, a, b):
        return abs(a - b)


class TestDenGraphIO(unittest.TestCase):
    #: the distance function/class with which to test
    distance_cls = DeltaDistance

    @staticmethod
    def random_nodes(length, base):
        return [random.randint(base, 2*base) for _ in range(length)]

    def test_noise(self):
        graph = DistanceGraph(
            nodes=[1, 2, 3, 4, 5, 6, 20],
            distance=self.distance_cls(),
            symmetric=True
        )
        io_graph = DenGraphIO(
            base_graph=graph,
            cluster_distance=5,
            core_neighbours=5
        )
        self.assertEqual(set([20]), io_graph.noise)

    def test_simple_graph(self):
        graph = DistanceGraph(
            nodes=self.random_nodes(100, 10) + self.random_nodes(100, 40),
            distance=self.distance_cls(),
            symmetric=True
        )
        io_graph = DenGraphIO(
            base_graph=graph,
            cluster_distance=5,
            core_neighbours=5
        )
        self.assertEqual(2, len(io_graph.clusters))

    def test_overlapping_clusters(self):
        nodes = [1, 2, 3, 4, 5, 6, 9, 14, 15, 16, 17, 18, 19, 20]
        graph = self._validation_graph_for_nodes(
            nodes=nodes,
            distance=self.distance_cls,
            cluster_distance=5,
            core_neighbours=5
        )
        self.assertEqual([4, 5, 6, 14], graph.graph.get_neighbours(9, 5))
        self.assertEqual(2, len(graph.clusters))
        self.assertEqual([set([9]), set([9])], [cluster.border_nodes for cluster in graph.clusters])

    def test_simple_incremental_behaviour(self):
        nodes = [1, 2, 3, 4, 5, 6]
        validation_io_graph = self._validation_graph_for_nodes(
            nodes=nodes,
            distance=self.distance_cls,
            cluster_distance=5,
            core_neighbours=5)

        graph = DistanceGraph(
            nodes=[],
            distance=self.distance_cls(),
            symmetric=True
        )
        io_graph = DenGraphIO(
            base_graph=graph,
            cluster_distance=5,
            core_neighbours=5
        )
        self.assertEqual(0, len(io_graph.clusters))
        for node in nodes:
            io_graph[node] = {}

        self.assertEqual(len(validation_io_graph.clusters), len(io_graph.clusters))
        self.assertEqual(validation_io_graph, io_graph)

    def test_add_incremental_behaviour(self):
        base_nodes = [1, 2, 3, 4, 5, 6, 7, 8]
        nodes_to_add = [10]
        validation_io_graph = self._validation_graph_for_nodes(
            nodes=base_nodes + nodes_to_add,
            distance=self.distance_cls,
            cluster_distance=5,
            core_neighbours=5
        )

        graph = DistanceGraph(
            nodes=base_nodes,
            distance=self.distance_cls(),
            symmetric=True
        )
        io_graph = DenGraphIO(
            base_graph=graph,
            cluster_distance=5,
            core_neighbours=5
        )
        for node in nodes_to_add:
            io_graph[node] = {}
        self.assertEqual(validation_io_graph, io_graph)

    def test_simple_noise(self):
        io_graph = DenGraphIO(
            base_graph=DistanceGraph(
                nodes=[1, 2, 3, 4, 5, 6],
                distance=self.distance_cls(),
                symmetric=True
            ),
            core_neighbours=5,
            cluster_distance=5
        )
        self.assertEqual(1, len(io_graph.clusters))
        self.assertEqual(set(), io_graph.noise)

        io_graph._recluster(io_graph.clusters[0])
        self.assertEqual(1, len(io_graph.clusters))
        self.assertEqual(set(), io_graph.noise)

    def test_noise_removal(self):
        base_nodes = [1, 2, 3, 4, 5, 6, 7, 8]
        remove_nodes = [30, 31]
        validation_io_graph = self._validation_graph_for_nodes(
            nodes=base_nodes,
            distance=self.distance_cls,
            cluster_distance=5,
            core_neighbours=5
        )
        graph = DistanceGraph(
            nodes=base_nodes+remove_nodes,
            distance=self.distance_cls(),
            symmetric=True
        )
        io_graph = DenGraphIO(
            base_graph=graph,
            cluster_distance=5,
            core_neighbours=5
        )
        self.assertTrue(all([node in io_graph.noise for node in remove_nodes]))
        for node in remove_nodes:
            del io_graph[node]
        self.assertEqual(validation_io_graph, io_graph)
        self.assertTrue(all([node not in io_graph.noise for node in remove_nodes]))

    def test_remove_downgrade_behaviour(self):
        base_nodes = [1, 3, 4, 5, 6, 7, 13, 14, 15, 16, 17, 18]
        remove_nodes = [2]
        validation_io_graph = self._validation_graph_for_nodes(
            nodes=base_nodes,
            distance=self.distance_cls,
            cluster_distance=5,
            core_neighbours=5
        )
        graph = DistanceGraph(
            nodes=base_nodes + remove_nodes,
            distance=self.distance_cls(),
            symmetric=True
        )
        io_graph = DenGraphIO(
            base_graph=graph,
            cluster_distance=5,
            core_neighbours=5
        )
        for node in remove_nodes:
            del io_graph[node]
        self.assertEqual(validation_io_graph, io_graph)

    def test_remove_incremental_behaviour(self):
        base_nodes = [1, 2, 3, 4, 5, 6, 12, 13, 14, 15, 16, 17]
        remove_nodes = [7]
        validation_io_graph = self._validation_graph_for_nodes(
            nodes=base_nodes,
            distance=self.distance_cls,
            cluster_distance=5,
            core_neighbours=5
        )
        graph = DistanceGraph(
            nodes=base_nodes + remove_nodes,
            distance=self.distance_cls(),
            symmetric=True
        )
        io_graph = DenGraphIO(
            base_graph=graph,
            cluster_distance=5,
            core_neighbours=5
        )
        for node in remove_nodes:
            del io_graph[node]
            self.assertEqual(validation_io_graph, io_graph)

    def test_remove_cluster(self):
        base_nodes = [1, 2, 3, 4, 5]
        remove_nodes = [6]
        validation_io_graph = self._validation_graph_for_nodes(
            nodes=base_nodes,
            distance=self.distance_cls,
            cluster_distance=5,
            core_neighbours=5
        )
        io_graph = DenGraphIO(
            base_graph=DistanceGraph(
                nodes=base_nodes + remove_nodes,
                distance=self.distance_cls(),
                symmetric=True),
            cluster_distance=5,
            core_neighbours=5
        )
        for node in remove_nodes:
            del io_graph[node]
        self.assertEqual(validation_io_graph, io_graph)

    def test_real_world_example(self):
        class DistanceMatrixDistance(object):
            is_symmetric = True
            matrix = []

            def __call__(self, a, b):
                return self.matrix[a-1][b-1]

        file_path = os.path.join(
            os.path.dirname(dengraph_unittests.__file__),
            "data/tree_distances.csv.zip"
        )
        distance = DistanceMatrixDistance()
        nodes = None
        with zipfile.ZipFile(file_path) as zipped_data:
            with zipped_data.open("tree_distances.csv") as tree_file:
                if sys.version_info >= (3,):
                    tree_file = io.TextIOWrapper(tree_file)
                csvreader = csv.reader(tree_file, delimiter=",")
                header_initialized = False
                for row in csvreader:
                    try:
                        if row[0].startswith("#"):
                            continue
                    except IndexError:
                        pass
                    if not header_initialized:
                        nodes = [int(element) for element in row]
                        header_initialized = True
                        continue
                    distance.matrix.append([float(element) for element in row])
        dengraph = DenGraphIO(
            base_graph=DistanceGraph(
                nodes=nodes,
                distance=distance,
                symmetric=True
            ),
            core_neighbours=5,
            cluster_distance=.00001
        )
        print(len(dengraph.clusters))

    def _validation_graph_for_nodes(self, distance, nodes, cluster_distance, core_neighbours, graph_type=DistanceGraph):
        graph = graph_type(
            nodes=nodes,
            distance=distance(),
            symmetric=True
        )
        return DenGraphIO(
            base_graph=graph,
            cluster_distance=cluster_distance,
            core_neighbours=core_neighbours
        )
