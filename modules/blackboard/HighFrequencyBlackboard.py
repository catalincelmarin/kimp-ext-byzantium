
from typing import Any

from kimera.helpers.Helpers import Helpers
from kimera.store.StoreFactory import StoreFactory
from .InMemoryBlackboard import InMemoryBlackboard


class HighFrequencyBlackboard(InMemoryBlackboard):
    """
    High-frequency blackboard that uses Redis hashes for atomic field updates.
    Each 'key' is treated as a logical entity; values are flat key-value fields.
    """

    def __init__(self, namespace: str, connection_name=None):
        super().__init__()
        self.namespace = namespace
        self._store = StoreFactory.get_mem_store(
            namespace=f"hf:{namespace}",
            connection_name=connection_name
        )

    def _key(self, key: str) -> str:
        return f"hf:{self.namespace}:{key}"

    def set(self, key: str, value: Any):
        """
        Set multiple fields for an entity. Assumes value is a dict.
        Overwrites existing fields with new values.
        """

        try:
            use_value = value

            if not isinstance(value, dict):
                use_value = {"__value":value if not isinstance(value,bool) else int(value) }
            self._store.hset(key, mapping=use_value)
        except Exception as e:
            print(e)
            print(value)
            raise e

    def get(self, key: str) -> dict | None:
        """
        Get all fields of the entity stored under `key`.
        """
        data = self._store.hgetall(key)

        if data:
            for key,value in data.items():
                if key == "__value":
                    return value
            return data

        return None

    def remove(self, key: str):
        """
        Remove all fields (entire hash) for the key.
        """
        self._store.delete(key)

    def clear(self,delete=False):
        """
        Flush all tracked entities under this namespace.
        """
        Helpers.sysPrint("NAMESPACE TO DELETE",self.namespace)
        keys = self._store.keys(f"*")
        for raw_key in keys:
            key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
            logical_key = key.split(":")[-1]
            self.remove(logical_key)
            self._types.clear()
        if delete:
            pass
            #self._store.flush()

    def dump(self) -> dict:
        """
        Get a snapshot of all tracked entities and their fields.
        """
        keys = self._store.keys(f"*")
        result = {}
        for raw_key in keys:
            key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
            logical_key = key.split(":")[-1]
            result[logical_key] = self.get(logical_key)
        return result

    @classmethod
    def from_dump(cls, data: dict, namespace="default", connection_name=None):
        """
        Rebuild the Redis-backed blackboard from a dumped memory state.
        This writes to Redis, not just in-memory.

        :param data: Dict in the form {key1: {field1: value1, ...}, ...}
        :param connection_name memstore connection
        :param namespace memstore namespace
        :return: New HighFrequencyBlackboard instance
        """
        instance = cls(namespace=namespace, connection_name=connection_name)

        for key, value in data.items():
            if not isinstance(value, dict):
                raise ValueError(
                    f"[HighFrequencyBlackboard.from_dump] Expected dict for key '{key}', got {type(value).__name__}")
            instance._store.hset(key, mapping=value)

        return instance

