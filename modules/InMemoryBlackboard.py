from app.ext.byzantium.modules.SynodeBlackboard import SynodeBlackboard


class InMemoryBlackboard(SynodeBlackboard):
    """
    Basic dictionary-backed implementation of SynodeBlackboard.
    This is not thread-safe — suitable for single-threaded environments or prototyping.
    """

    def __init__(self):
        self._store = {}

    def get(self, key: str):
        return self._store.get(key, None)

    def set(self, key: str, value):
        self._store[key] = value

    def has(self, key: str) -> bool:
        return key in self._store

    def remove(self, key: str):
        if key in self._store:
            del self._store[key]

    def keys(self) -> list[str]:
        return list(self._store.keys())

    def dump(self) -> dict:
        return dict(self._store)  # Return a shallow copy

    @classmethod
    def from_dump(cls, data: dict):
        instance = cls()
        instance._store = data
        return instance