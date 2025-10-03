from enum import Enum


class Signals(int, Enum):
    HALT = 0
    SKIP = 1
    PASS = 2
    BREAK = 3
    EXIT = 4
    ERROR = 5

class OperatorTypes(str, Enum):
    BOT = "bot"
    SYNOD = "synod"
    BASIC = "basic"
    HYDRA = "hydra"
    ARGUS = "argus"

# -- Operation Types --
class SynodeOpType(str, Enum):
    FORK_TO = 'fork_to'
    CHAIN_TO = 'chain_to'
    LOOP_TO = 'loop_to'
    FILTER = 'filter'
    MAP = 'map'
    REDUCE = 'reduce'