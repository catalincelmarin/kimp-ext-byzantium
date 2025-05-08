import yaml

from .SynodeDict import SynodeDict
from .SynodeList import SynodeList
from .SynodeSet import SynodeSet

from collections import defaultdict, deque

"""
    Perpetuals are constants, they escape resets.
"""

class SynodeTypeLoader(yaml.SafeLoader):
    @staticmethod
    def val_constructor(loader, node):
        """Handles regular dynamic types."""
        type_str = loader.construct_scalar(node)
        if type_str == "list":
            return []
        elif type_str == "dict":
            return {}
        elif type_str == "set":
            return set()
        elif type_str == "tuple":
            return tuple()
        elif type_str == "defaultdict:int":
            return defaultdict(int)
        else:
            raise ValueError(f"Unsupported !let type: {type_str}")

    @staticmethod
    def ref_constructor(loader, node):
        """Handles perpetual Synode types."""
        type_str = loader.construct_scalar(node)
        if type_str == "list":
            return SynodeList()
        elif type_str == "dict":
            return SynodeDict()
        elif type_str == "set":
            return SynodeSet()
        elif type_str == "queue":
            return deque()
        else:
            raise ValueError(f"Unsupported !const type: {type_str}")

# Register the constructors
yaml.add_constructor('!val', SynodeTypeLoader.val_constructor, Loader=SynodeTypeLoader)
yaml.add_constructor('!ref', SynodeTypeLoader.ref_constructor, Loader=SynodeTypeLoader)
