from collections import deque
from typing import Any

from .Blackboard import Blackboard
from .types.SynodeDict import SynodeDict
from .types.SynodeList import SynodeList
from .types.SynodeSet import SynodeSet
from .types.SynodeTypeMap import SynodeTypeMap


class InMemoryBlackboard(Blackboard):
    """
    Basic dictionary-backed implementation of Blackboard.
    This is not thread-safe — suitable for single-threaded environments or prototyping.
    """

    def __init__(self):
        self._store = {}
        self._types = {}

    def get(self, key: str):
        return self._store.get(key, None)

    def set(self, key: str, value: Any):
        self._types[key] = type(value)

        if self.has(key):
            current_value = self._store[key]

            if isinstance(current_value, SynodeList):
                if isinstance(value, (list,set,tuple)):
                    current_value.extend(value)
                else:
                    current_value.append(value)

            elif isinstance(current_value, SynodeDict) and isinstance(value, dict):
                current_value.update(value)

            elif isinstance(current_value, SynodeSet):
                if isinstance(value, (list, set,tuple)):
                    for item in value:
                        current_value.add(item)
                else:
                    current_value.add(value)

            elif isinstance(current_value, deque):
                if isinstance(value, (list,set, tuple)):
                    for item in value:
                        current_value.append(item)
                else:
                    current_value.append(value)

            else:
                # Regular list, set, dict, or anything else => reset
                self._store[key] = value

        else:
            self._store[key] = value

    def clear(self):
        self._store.clear()

    def has(self, key: str) -> bool:
        return key in self._store

    def remove(self, key: str):
        if key in self._store:
            del self._store[key]

    def keys(self) -> list[str]:
        return list(self._store.keys())

    def dump(self) -> dict:
        result = {}
        for k, v in self._store.items():
            if isinstance(v, (set, SynodeSet, deque)):
                result[k] = list(v)  # convert to list
            elif isinstance(v, tuple):
                result[k] = list(v)  # also convert tuples
            else:
                result[k] = v
        return result  # Return a shallow copy


    def full_dump(self):
        return {
            "store": self.dump(),
            "types": self._types
        }


    @classmethod
    def from_dump(cls, data: dict):
        instance = cls()
        store_data = data.get("store", {})
        types_data = data.get("types", {})

        for k, v in store_data.items():
            type_name = types_data.get(k)

            real_type = SynodeTypeMap.TYPE_MAP.get(type_name)

            if real_type:
                if real_type in [set, SynodeSet]:
                    instance._store[k] = set(v)
                elif real_type == deque:
                    instance._store[k] = deque(v)
                elif real_type == tuple:
                    instance._store[k] = tuple(v)
                elif real_type in [SynodeList, SynodeDict]:
                    instance._store[k] = real_type(v)
                else:
                    instance._store[k] = v
                instance._types[k] = real_type
            else:
                # Fallback: keep value as is
                instance._store[k] = v
                instance._types[k] = type(v)

        return instance

