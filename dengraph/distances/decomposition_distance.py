from __future__ import absolute_import

import math

import dengraph.distance
from dengraph.clusterrepresentative import ClusterRepresentative

from dengraph.utilities.assess import ClusterRepresentativeWrapper, \
    PrototypeClusterRepresentativeWrapper


class DecompositionDistance(dengraph.distance.IncrementalDistance):
    """
    The Decomposition Distance relies on the distances and concepts developed for distance
    calculation for dynamic trees in the context of the ASSESS framework. Therefore, the usability
    of the given objects needs to satisfy the needs and interfaces of ASSESS.

    Attention: This class currently does not consider EnsembleSignatures!
    """
    def __init__(self, distance):
        self._distance = distance

    def __call__(self, first, second, default=None):
        """
        Method determines distance between first and second. First is expected to be the Cluster
        Representative and therefore, is considered as being 1 to many trees. For all those trees,
        the distance to second is determined. In the end, only the smallest of the calcualted
        distances is relevant for either classification or clustering.

        :param first: The Cluster Representative if any, otherwise a simple Identity Cache
        :param second: A simple Identity Cache
        :param default: Default is not considered here
        :return: Minimum distance given by the Cluster Representatives.
        """
        wrapped_first = PrototypeClusterRepresentativeWrapper.factory(first)
        wrapped_second = ClusterRepresentativeWrapper.factory(second)
        prototypes = wrapped_first.prototypes
        self._distance.init_distance(prototypes, wrapped_first)
        for signature in wrapped_second:
            matching_prototypes = wrapped_first.get(signature=signature)
            supporters = wrapped_second.get(signature=signature)
            for support_key, statistics in supporters.items():
                if len(statistics.keys()) <= 1:
                    # I am only considering count here
                    stat_key = "count"
                else:
                    # only considering duration here
                    stat_key = "duration"
                statistic = statistics.get(stat_key, [])
                for stat in statistic:
                    count = int(math.ceil(stat.count))
                    for _ in range(count):
                        self._distance.update_distance(
                            prototypes=prototypes,
                            signature_prototypes=wrapped_first,
                            event_type=support_key,
                            matches=[{signature: matching_prototypes}],
                            value=stat.value
                        )
        self._distance.finish_distance(prototypes, wrapped_first)
        result_list = self._distance.distance_for_prototypes(prototypes)[0]  # first ensemble
        event_count = self._distance.event_count()
        prototype_event_count = self._distance.node_count(prototypes=prototypes, signature_prototypes=wrapped_first)
        results = [result_list[index] / float(event_count[0] + prototype_event_count[index]) for index in range(len(prototypes))]
        return min(results)

    def mean(self, *args, **kwargs):
        # check, if the cluster already stores the mean required
        if len(args) == 1:
            args = args[0]
        try:
            cr = args.cluster_representative
            if cr is not None:
                changes = kwargs.get("changes", [])
                for change in changes:
                    cr[change] = None
                return cr
        except AttributeError:
            pass
        return ClusterRepresentative(args)
