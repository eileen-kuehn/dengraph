class IdentityCache:
    def __init__(self, parent):
        self.parent = parent
        self._data = {}  # new content to apply
        self._whiteout = set()  # keys to delete

    def __contains__(self, item):
        return item not in self._whiteout and (item in self._data or item in self.parent)

    def __getitem__(self, key):
        if key in self._whiteout:
            raise KeyError(key)
        try:
            return self._data[key]
        except KeyError:
            return self.parent[key]

    def __setitem__(self, key, value):
        self._data[key] = value
        try:
            self._whiteout.remove(key)
        except KeyError:
            pass

    def __delitem__(self, key):
        self._whiteout.add(key)

    def __iter__(self):
        # yield overwritten data first and do not repeat keys
        for key in self._data:
            if key not in self._whiteout:
                yield key
        for key in self.parent:
            if key not in self._whiteout and key not in self._data:
                yield key

    def __len__(self):
        return len(set(self))

    def __str__(self):
        whiteouts = ", ".join('%s: <~>' % key for key in self._whiteout)
        key_values = repr({key: self[key] for key in self._data if key not in self._whiteout})
        if whiteouts and key_values:
            content = '{%s, %s}' % (whiteouts, key_values[1:-1])
        elif whiteouts:
            content = '{%s}' % whiteouts
        elif key_values:
            content = '{%s}' % key_values[1:-1]
        else:
            content = '{}'
        return '%s >> %s' % (content, self.parent)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self)

    def keys(self):
        keys = []
        keys.extend(self._data.keys())
        keys.extend(self.parent.keys())
        return keys

    def items(self):
        items = []
        items.extend(self._data.items())
        items.extend(self.parent.items())
        return items

    def values(self):
        values = []
        values.extend(self._data.values())
        values.extend(self.parent.values())
        return values
