from typing import List, Dict, Optional, Any, Callable, Awaitable, Union
from pydantic import BaseModel, Field

from ..blackboard.BlackboardSchemas import BlackboardInit


class ArgusHookSchema(BaseModel):
    keys: List[str]
    hook: str  # Path as a string initially, resolve later dynamically

class ArgusWatchSchema(BaseModel):
    key: str
    expression: str
    default: Optional[Any] = None

class StalkerSchema(BaseModel):
    stalker: str
    name: str
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    heartbeat: Optional[float] = 1
    startup: bool = True

class ArgusSchema(BaseModel):
    module: str
    name: str
    heartbeat: Optional[float] = 1
    blackboard: Optional[BlackboardInit] = None
    stalkers: List[StalkerSchema]
    watch: List[ArgusWatchSchema] = Field(default_factory=list)
    hooks: List[ArgusHookSchema] = Field(default_factory=list)
