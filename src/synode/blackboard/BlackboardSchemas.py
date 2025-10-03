from typing import Union, Type, Dict, Any, Optional

from pydantic import BaseModel, Field, model_validator

from .BBHelpers import BBHelpers
from .Blackboard import Blackboard
from .HighFrequencyBlackboard import HighFrequencyBlackboard
from .InMemoryBlackboard import InMemoryBlackboard
from .SharedBlackboard import SharedBlackboard


class BlackboardInit(BaseModel):
    blackboard_module: Union[str, Type[Blackboard]] = Field(..., alias="type")
    kwargs: Optional[Dict[str, Any]] = Field(default_factory=dict)


    @model_validator(mode="before")
    def resolve_blackboard_module(cls, data: Dict[str, Any]):

        if isinstance(data, dict):
            module = data.get("type")

            if isinstance(module, str):
                if module == "default":
                    data["type"] = InMemoryBlackboard
                elif module == "shared":
                    data["type"] = SharedBlackboard
                elif module == "hf":
                    data["type"] = HighFrequencyBlackboard
                else:
                    raise ValueError(f"Invalid blackboard_module string '{module}': must be 'default' or 'shared'")
            else:
                data["type"] = BBHelpers.get_class(module)
        return data
