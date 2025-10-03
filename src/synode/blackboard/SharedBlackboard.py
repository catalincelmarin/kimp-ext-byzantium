import uuid
from kimera.helpers.Helpers import Helpers
from kimera.store.StoreFactory import StoreFactory

from .InMemoryBlackboard import InMemoryBlackboard


class SharedBlackboard(InMemoryBlackboard):
    """
    InMemoryBlackboard extension that syncs automatically with a MemStore.
    MemStore acts like a fast namespace-isolated in-memory Redis.
    Data is serialized using Pickle for full Python object fidelity.
    """

    def __init__(self, namespace, connection_name=None):
        super().__init__()
        # Note: each instance has a unique namespace extension
        self.cache = StoreFactory.get_mem_store(
            namespace=f"{namespace}",
            connection_name=connection_name
        )

    def set(self, key: str, value):
        """
        Set a value in both local store and MemStore.
        """

        self.cache.set(key, value)  # MemStore handles pickle + base64

    def get(self, key: str, default_value=None):
        """
        Get a value from local store or fallback to MemStore.
        """

        cached = self.cache.get(key)
        if cached is not None:
            self._store[key] = cached  # cache locally
            return cached

        return default_value

    def remove(self, key: str):
        """
        Remove a key from both local memory and MemStore.
        """
        super().remove(key)
        self.cache.delete(key)

    def clear(self,delete=True):
        """
        Flush both local memory and MemStore namespace.
        """
        self.cache.flush()
        if delete:
            self._store.clear()
            self._types.clear()

    def dump(self) -> dict:
        """
        Dump local + shared keys, avoiding double-decoding.
        """
        result = dict(self._store)

        try:
            keys = self.cache.keys()
        except Exception:
            keys = []


        for key in keys:
            if isinstance(key, bytes):
                key = key.decode("utf-8")

            simple_key = key.split(":")[-1]  # remove namespace



            try:
                value = self.get(simple_key)
                if value is not None:
                    result[simple_key] = value
            except Exception as e:
                raise ValueError(f"[SharedBlackboard] Failed to decode key '{simple_key}': {e}")

        return result

    @classmethod
    def from_dump(cls, data: dict):
        """
        Rebuild the local blackboard from a dumped memory state.
        MemStore is not automatically repopulated.
        """
        instance = super().from_dump(data)
        return instance
