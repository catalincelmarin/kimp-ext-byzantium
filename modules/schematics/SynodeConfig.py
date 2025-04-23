from typing import List, Dict, Any, Optional, Union, Type

from kimera.helpers.Helpers import Helpers
from pydantic import BaseModel, Field, ValidationError, model_validator
from enum import Enum

from app.ext.byzantium.modules.InMemoryBlackboard import InMemoryBlackboard
from app.ext.byzantium.modules.Synode import Synode, SynodeImpl
from app.ext.byzantium.modules.helpers.Helpers import Helpers as SynodeHelpers
from app.ext.byzantium.modules.SynodeBlackboard import SynodeBlackboard


# assuming abstract base
# No SynodeImpl here, factory decides that


# -- Operation Types --
class SynodeOpType(str, Enum):
    FORK_TO = 'fork_to'
    CHAIN_TO = 'chain_to'
    LOOP_TO = 'loop_to'
    FILTER = 'filter'
    MAP = 'map'
    REDUCE = 'reduce'

# -- Operation Representation --
class SynodeOp(BaseModel):
    op_type: SynodeOpType
    target: Union[str, List[str]]
    before: Optional[str] = None
    after: Optional[str] = None
    kwargs: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_required_kwargs(self) -> "SynodeOp":
        if self.op_type == SynodeOpType.LOOP_TO:
            if "max_cycles" not in self.kwargs:
                raise ValueError("`max_cycles` is required in `kwargs` when `op_type` is LOOP_TO")
        if self.op_type == SynodeOpType.REDUCE:
            if "accumulator" not in self.kwargs:
                raise ValueError("`accumulator` is required in `kwargs` when `op_type` is REDUCE")
        return self

    def __repr__(self):
        return f"SynodeOp(op_type={self.op_type}, target={self.target}, kwargs={self.kwargs})"

class OperatorTypes(str, Enum):
    BOT = "bot"
    SYNOD = "synod"
    BASIC = "basic"
    HYDRA = "hydra"


class OperatorHandler(BaseModel):
    kwargs: Optional[Dict[str,Any]] = Field(default_factory=dict)
    description: Optional[str] = "no description provided"


class Operator(BaseModel):
    operator_type: OperatorTypes
    alias: str
    semaphore: Optional[int] = 30
    operator_path: str
    handlers: Optional[Dict[str,OperatorHandler]] = Field(default_factory=dict)
    kwargs: Optional[Dict[str, Any]] = Field(default_factory=dict)  # kwargs initialized as a new dict if None

    @model_validator(mode="before")
    def _normalize_handlers(cls, values):
        h = values.get("handlers")
        # if user passed a list of strings, convert to dict{name: {}}
        if isinstance(h, list):
            values["handlers"] = {name: {} for name in h}
        return values




# -- Agent Representation --
class SynodeAgent(BaseModel):
    agent: str
    timeout: Optional[int] = 30
    operator: str
    before: Optional[str] = None
    after: Optional[str] = None
    store_key: Optional[str] = None
    run_async: Optional[bool] = False
    async_callback:Optional[str] = None
    instructions: str
    kwargs: Optional[Dict[str,Any]] = Field(default_factory=dict)
    operations: List[SynodeOp] = Field(default_factory=list)
    default_value: Optional[Union[Any, None]] = None

    class Config:
        arbitrary_types_allowed = True

class BotReg(BaseModel):
    alias: str
    module_class: str

class BasicReg(BaseModel):
    method: str
    class_path: str

class SynodReg(BaseModel):
    alias: str
    synod_path: str

# -- Full SynodeConfig --
class SynodeConfig(BaseModel):
    name: str
    module_class: Type[Synode]  # <-- not a str, an actual class instance
    blackboard: Optional[Type[SynodeBlackboard]] = None
    description: str
    instructions: str
    run_async: Optional[bool] = False
    async_callback: Optional[str] = None
    triggers: List[str]
    operators: List[Operator] = Field(default_factory=list)  # Default empty list
    synode: List[SynodeAgent]

    @model_validator(mode='before')
    def set_default_module_class(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if "module_class" not in values or values["module_class"] is None:
            values["module_class"] = SynodeImpl

        if "blackboard" in values:
            Helpers.sysPrint("BLASS",values["blackboard"])
            if values["blackboard"] == "default":
                values["blackboard"] = InMemoryBlackboard
            elif values["blackboard"]:
                b_board = SynodeHelpers.get_class(values["blackboard"])
                values["blackboard"] = b_board
            else:
                values["blackboard"] = None

        return values

    @classmethod
    def from_config(cls, config: Dict[str, Any], module_class: Type[Synode]) -> "SynodeConfig":
        try:
            parsed_agents = [cls._parse_agent(agent,config.get("run_async",False)) for agent in config.get("synode", [])]
            for operator in config.get("operators", []):
                if operator.get("operator_type") == OperatorTypes.SYNOD.value:
                    pieces = operator.get("operator_path").split(".")
                    if pieces[-1] != "yaml":
                        result = f"synod.{pieces[-1]}.yaml"

                        operator["operator_path"] = "/".join([*pieces[:-1],result])

            return cls(
                name=config["name"],
                module_class=module_class,  # <- already resolved by SynodeFactory
                description=config["description"],
                async_callback=config.get("async",None),
                blackboard=config.get("blackboard",None),
                instructions=config["instructions"],
                run_async=config.get("run_async",False),
                triggers=config.get("triggers", []),
                operators=config.get("operators", []),
                synode=parsed_agents
            )

        except Exception as e:
            print("Validation error:", e)
            raise e

    @classmethod
    def _parse_agent(cls, raw: Dict[str, Any],default_run_async: bool) -> SynodeAgent:
        def build_op(op_data: Dict[str, Any]) -> SynodeOp:
            return SynodeOp(
                op_type=SynodeOpType(op_data["op_type"]),
                target=op_data["target"],
                before=op_data.get("before",None),
                after=op_data.get("after",None),
                kwargs=op_data.get("kwargs", {})
            )

        operations = [build_op(op) for op in raw.get("operations", [])]

        return SynodeAgent(
            agent=raw["agent"],
            operator=raw.get("operator"),
            before=raw.get("before"),
            after=raw.get("after"),
            timeout=raw.get("timeout",30),
            async_callback=raw.get("async",None),
            run_async=raw.get("run_async", default_run_async),
            store_key=raw.get("store_key"),
            instructions=raw.get("instructions",""),
            operations=operations,
            kwargs=raw.get("kwargs", {}),
            default_value=raw.get("default_value",None)
        )