from __future__ import absolute_import

from assess.algorithms.signatures.signaturecache import PrototypeSignatureCache, SignatureCache

from dengraph.clusterrepresentative import ClusterRepresentative


class ClusterRepresentativeWrapper(object):
    def factory(cluster_representative):
        if isinstance(cluster_representative, SignatureCache):
            return SignatureCacheWrapper(cluster_representative)
        if isinstance(cluster_representative, ClusterRepresentative):
            return IdentityCacheWrapper(cluster_representative)
    factory = staticmethod(factory)


class PrototypeClusterRepresentativeWrapper(object):
    def factory(cluster_representative):
        if isinstance(cluster_representative, SignatureCache) or \
                isinstance(cluster_representative, PrototypeSignatureCache):
            return PrototypeSignatureCacheWrapper(cluster_representative)
        if isinstance(cluster_representative, ClusterRepresentative):
            return IdentityCacheWrapper(cluster_representative)
    factory = staticmethod(factory)


class SignatureCacheWrapper(object):
    def __init__(self, cache):
        self._signature_cache = cache

    def __iter__(self):
        return iter(self._signature_cache)

    @property
    def statistics_cls(self):
        return self._signature_cache.statistics_cls

    def node_count(self):
        if isinstance(self._signature_cache, PrototypeSignatureCache):
            return self._signature_cache.node_count().values()[0]
        return self._signature_cache.node_count()

    def get(self, signature):
        if isinstance(self._signature_cache, PrototypeSignatureCache):
            return self._signature_cache.get(signature=signature).values()[0]
        return self._signature_cache.get(signature=signature)

    def multiplicity(self, signature=None, event_type=None, **kwargs):
        return self._signature_cache.multiplicity(signature=signature, event_type=event_type)


class PrototypeSignatureCacheWrapper(SignatureCacheWrapper):
    @property
    def prototypes(self):
        return [self._signature_cache]

    def node_count(self, prototype=None):
        if isinstance(self._signature_cache, PrototypeSignatureCache):
            return self._signature_cache.node_count(prototype)
        return self._signature_cache.node_count()

    def get(self, signature):
        if isinstance(self._signature_cache, PrototypeSignatureCache):
            try:
                return {self._signature_cache: self._signature_cache.get(signature)}
            except IndexError:
                return {}
        result = self._signature_cache.get(signature)
        if result is not None:
            return {self._signature_cache: result}
        return {}


class IdentityCacheWrapper(ClusterRepresentativeWrapper):
    def __init__(self, cache):
        self._wrapped_object = cache

    @property
    def prototypes(self):
        return self._wrapped_object.data

    def node_count(self, prototype=None):
        return len(prototype)

    def get(self, signature):
        return {data: data[signature] for data in self._wrapped_object if signature in data}

    def multiplicity(self, signature=None, event_type=None, prototype=None):
        # TODO: if prototype, then use to get multiplicity
        return len(self._wrapped_object.data[0])
