import base64
import pickle
import uuid

from kimera.store.StoreFactory import StoreFactory

from app.ext.byzantium.modules.blackboard.InMemoryBlackboard import InMemoryBlackboard



class SharedBlackboard(InMemoryBlackboard):
    """
    InMemoryBlackboard extension that syncs automatically with a MemStore.
    MemStore acts like a fast namespace-isolated in-memory Redis.
    Data is serialized using Pickle for full Python object fidelity.
    """

    def __init__(self, namespace, connection_name=None):
        super().__init__()
        self.cache = StoreFactory.get_mem_store(namespace=namespace + f"::{uuid.uuid4()}", connection_name=connection_name)

    def set(self, key: str, value):
        super().set(key, value)
        pickled = pickle.dumps(self._store[key])
        encoded = base64.b64encode(pickled).decode("ascii")  # Base64 => safe string
        self.cache.set(key, encoded)

    def get(self, key: str):
        local = super().get(key)
        if local is not None:
            return local

        cached = self.cache.get(key)
        if cached is not None:
            if isinstance(cached, str):
                cached = base64.b64decode(cached.encode("ascii"))  # Base64 decode
            value = pickle.loads(cached)
            self._store[key] = value
            return value

        return None

    def remove(self, key: str):
        """
        Remove a key from both local memory and MemStore.
        """
        super().remove(key)
        self.cache.delete(key)

    def clear(self):
        """
        Flush both local memory and the MemStore namespace.
        """
        self.cache.flush()
        self._store.clear()
        self._types.clear()

    def dump(self) -> dict:
        """
        Dump the current local memory (no MemStore dump).
        """
        return super().dump()

    @classmethod
    def from_dump(cls, data: dict):
        """
        Rebuild the local blackboard from a dumped memory state.
        MemStore is not automatically repopulated.
        """
        instance = super().from_dump(data)
        return instance
