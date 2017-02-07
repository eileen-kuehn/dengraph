from __future__ import absolute_import
from dengraph.dengraph import DenGraphIO
import dengraph.distance


class DenGraphFIO(DenGraphIO):
    def __init__(self, base_graph):
        try:
            if not isinstance(base_graph.distance, dengraph.distance.Distance):
                raise dengraph.distance.NoDistanceSupport
        except AttributeError:
            raise dengraph.distance.NoDistanceSupport
        core_neighbours = 4  # given in original DBScan paper, optimal for 2D clustering
        cluster_distance = .1  # educated guess?!
        super(DenGraphFIO, self).__init__(base_graph, cluster_distance, core_neighbours)

    def __setitem__(self, key, value):
        super(DenGraphFIO, self).__setitem__(key, value)
        for cluster in self.clusters_for_node(key):
            # we need to adapt or create cluster representative for current cluster
            try:
                cluster.cluster_representative
            except AttributeError:
                cr = self.graph.distance.mean(cluster)
                cluster.cluster_representative = cr
            else:
                self.graph.distance.mean(cluster, changes=[key])

    def _validate_cluster(self, cluster):
        distances = self._revalidate_cluster(cluster)
        print("distances %s" % distances)
        maximum_distance = max(distances)
        if maximum_distance > self.cluster_distance:
            print("---> CRs not adequate")

    def _validate_nodes(self, nodes):
        representative = self.graph.distance.mean(list(nodes))
        distances = []
        for node in nodes:
            distances.append(self.graph.distance(representative, node))
        return distances

    def _revalidate_cluster(self, cluster):
        representative = self.graph.distance.mean(cluster)
        distances = []
        for node in cluster.border_nodes:
            distances.append(self.graph.distance(representative, node))
            print("border: %s" % distances[-1])
        for node in cluster.core_nodes:
            distances.append(self.graph.distance(representative, node))
            print("core: %s" % distances[-1])
        return distances
