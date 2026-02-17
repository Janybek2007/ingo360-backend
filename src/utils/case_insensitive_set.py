class CaseInsensitiveSet:
    def __init__(self, data=None):
        self._data: set[str] = set()
        if data:
            self.update(data)

    def _normalize(self, value):
        return value.casefold() if isinstance(value, str) else value

    def add(self, value):
        self._data.add(self._normalize(value))

    def update(self, values):
        for v in values:
            self.add(v)

    def __contains__(self, value):
        return self._normalize(value) in self._data

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def clear(self):
        self._data.clear()