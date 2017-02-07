from __future__ import absolute_import

from dengraph.primitives.identitycache import IdentityCache


class ClusterRepresentative:
    """
    A Cluster Representative provides a unified view on several possible Cluster Representatives.
    Those Cluster Representatives are managed by Identity Caches. In clustering, we also support
    arbitrary shaped clusters, thus, we require a set of Cluster Representatives to represent this
    cluster. As I don't want the actual cluster to take care on the number of required cluster
    representatives, I currently consider one single cluster represenative that takes care of
    managing all underlying objects that are required to adhere to the given cluster distance.

    This single class also enables abstraction from internal logic when a new cluster representative
    needs to be created. This is not the task of the cluster but the actual business logic.
    """
    def __init__(self, iterable):
        self._root = IdentityCache({})
        self.data = []
        if iterable is not None:
            self._add_node(iterable)

    def __setitem__(self, key, value):
        downgraded = False
        root = self._root
        signature_cache = key
        to_handle = set(signature_cache)
        for signature in root:
            if signature in signature_cache:
                root[signature] = self._mean(
                    root[signature], [signature_cache.get(signature=signature)])
                to_handle.remove(signature)
            else:
                # downgrade signature
                downgraded = True
                root_value = root[signature]
                del root[signature]
                if len(self.data) > 0:
                    for data in self.data:
                        data[signature] = root_value  # FIXME: statistics here are wrong...
                else:
                    new_cache = IdentityCache(root)
                    self.data.append(new_cache)
                    new_cache[signature] = root_value  # those statistics are correct :)
        self._handle_remaining_signatures(to_handle, signature_cache, downgraded=downgraded)

    def __getitem__(self, item):
        pass

    def __iter__(self):
        for data in self.data:
            yield data

    def _add_node(self, iterable):
        signature_set = set()
        inserted_signatures = set()
        for signature_cache in iterable:
            signature_set.update(signature_cache)
        for signature in signature_set:
            occurrences = [1 if signature in cache else 0 for cache in iterable]
            if sum(occurrences) == len(iterable):
                self._root[signature] = self._mean({}, [cache.get(
                    signature=signature) for cache in iterable])
                inserted_signatures.add(signature)
        for signature_cache in iterable:
            to_handle = set(signature_cache) - inserted_signatures
            self._handle_remaining_signatures(to_handle, signature_cache)

    def _create_new_identity(self, to_handle, cache):
        """
        Method to insert a new IdentityCache object. It takes care to consider the correct parent
        for insertion and afterwards inserts all elements given in to_handle into the created
        IdentityCache.

        :param to_handle: Iterable giving the identities to be inserted
        """
        if len(self.data) == 0:
            parent = self._root
        else:
            parent = self.data[0].parent
        new_cache = IdentityCache(parent)
        self.data.append(new_cache)
        for element in to_handle:
            new_cache[element] = cache.get(signature=element)

    def _handle_remaining_signatures(self, to_handle, cache, downgraded=False):
        if downgraded is True:
            self._create_new_identity(to_handle, cache)
        else:
            for data in self.data:
                if all(element in data for element in to_handle):
                    for element in to_handle:
                        data[element] = self._mean(data[element], [cache.get(signature=element)])
                    break
            else:
                self._create_new_identity(to_handle, cache)

    def _mean(self, one, two):
        result = {}
        for element in two:
            for event_key, statistics in element.items():
                current_result = result.setdefault(event_key, {})
                for stat_key, statistic in statistics.items():
                    current_result[stat_key] = type(statistic).mean([one.get(event_key, {}).get(
                        stat_key, type(statistic)()), statistic])
        return result
