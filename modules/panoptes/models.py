from enum import Enum
from typing import Any

from pydantic import BaseModel

class ControlMessage(BaseModel):
    origin: str
    target: str  # recipient name ("stalker1", "argus", etc.)
    message: Any  # the actual message content


class QueueBound(str, Enum):
    INBOUND = 1
    OUTBOUND = -1