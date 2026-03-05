class CaseInsensitiveDict(dict):
    @staticmethod
    def _normalize_key(key):
        if isinstance(key, str):
            return key.casefold()
        if isinstance(key, tuple):
            return tuple(k.casefold() if isinstance(k, str) else k for k in key)
        return key

    def __init__(self, data=None, **kwargs):
        super().__init__()
        if data:
            for key, value in (data.items() if isinstance(data, dict) else data):
                self[key] = value
        for key, value in kwargs.items():
            self[key] = value

    def __getitem__(self, key):
        return super().__getitem__(self._normalize_key(key))

    def __setitem__(self, key, value):
        super().__setitem__(self._normalize_key(key), value)

    def __delitem__(self, key):
        super().__delitem__(self._normalize_key(key))

    def __contains__(self, key):
        return super().__contains__(self._normalize_key(key))

    def get(self, key, default=None):
        return super().get(self._normalize_key(key), default)

    def pop(self, key, *args):
        return super().pop(self._normalize_key(key), *args)
