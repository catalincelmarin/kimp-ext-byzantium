from collections import deque

from .SynodeDict import SynodeDict
from .SynodeList import SynodeList
from .SynodeSet import SynodeSet


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