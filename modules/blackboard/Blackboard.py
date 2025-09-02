from abc import ABC, abstractmethod
from typing import Any, Dict

from pydantic import BaseModel, Field


class BlackboardHandler(BaseModel):
    handler: str
    data: Dict[str,Any] = Field(default_factory=dict)

class Blackboard(ABC):
    """
    Abstract shared memory space where agents can store and retrieve information.
    Acts as a passive key-value store. No orchestration or logic is executed.
    """

    @abstractmethod
    def get(self, key: str, default_value=None):
        """Retrieve a value by key."""
        pass

    @abstractmethod
    def set(self, key: str, value):
        """Store a value by key."""
        pass

    @abstractmethod
    def has(self, key: str) -> bool:
        """Check if a key exists."""
        pass

    @abstractmethod
    def clear(self,delete=True):
        """clear blackboard"""
        pass

    @abstractmethod
    def remove(self, key: str):
        """Delete a key-value pair."""
        pass

    @abstractmethod
    def keys(self) -> list[str]:
        """Return all keys currently in the blackboard."""
        pass

    @abstractmethod
    def dump(self) -> dict:
        """Return the entire contents of the blackboard as a dictionary."""
        pass


    @classmethod
    def from_dump(cls, data: dict):
       pass



