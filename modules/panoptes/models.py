from enum import Enum

from pydantic import BaseModel

class ControlMessage(BaseModel):
    origin: str
    target: str  # recipient name ("stalker1", "argus", etc.)
    message: str  # the actual message content


class QueueBound(str, Enum):
    INBOUND = 1
    OUTBOUND = -1