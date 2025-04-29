from collections import deque

from app.ext.byzantium.modules.blackboard.types.SynodeDict import SynodeDict
from app.ext.byzantium.modules.blackboard.types.SynodeList import SynodeList
from app.ext.byzantium.modules.blackboard.types.SynodeSet import SynodeSet


class SynodeTypeMap:
    TYPE_MAP = {
        "list": list,
        "dict": dict,
        "set": set,
        "tuple": tuple,
        "deque": deque,
        "SynodeList": SynodeList,
        "SynodeDict": SynodeDict,
        "SynodeSet": SynodeSet
    }