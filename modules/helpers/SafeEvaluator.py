import re
from typing import Optional

import jmespath
from kimera.helpers.Helpers import Helpers

from ..blackboard.InMemoryBlackboard import InMemoryBlackboard


class SafeEvaluator:
    _VAR_PATTERN = re.compile(r"\$([a-zA-Z0-9_.\[\]]+)")  # precompiled regex

    def __init__(self, blackboard=None):
        self._blackboard = blackboard
        self._data = blackboard if blackboard else {}

    @staticmethod
    def interpolate(expression, merged_data):


        # Match innermost ($...) group, supports nested resolution
        pattern = re.compile(r"\(\$([^\(\)]+?)\)")

        def resolve(path: str):
            val = jmespath.search(path, merged_data)
            if val is None:
                raise KeyError(f"Could not resolve: {path}")
            return val

        def resolve_nested(expr: str):
            # Resolve all innermost ($...) recursively
            while True:
                match = pattern.search(expr)
                if not match:
                    break
                full = match.group(0)  # e.g., "($foo.bar)"
                inner = match.group(1)  # e.g., "foo.bar"
                result = resolve(inner)
                # Only allow non-dict replacements inside other expressions
                if isinstance(result, dict):
                    return expr
                expr = expr[:match.start()] + str(result) + expr[match.end():]
            return resolve(expr)

        # Start with top-level expression check
        expr = expression.strip()
        if expr.startswith("($") and expr.endswith(")"):
            return resolve_nested(expr[2:-1])
        else:
            return expr

    def eval(self, expression, private_board: Optional[InMemoryBlackboard],input_dict=None):
            if private_board is None:
                private_data = {}
            else:
                private_data = private_board.dump()

            base = self._data.dump() if hasattr(self._data, "dump") else self._data
            _merged_data = {**base, **private_data}

            if isinstance(input_dict,dict):
                _merged_data = {**_merged_data,"__input": input_dict}

            if expression.startswith("$"):
                expression = f"({expression})"

            def extract_top_level_expressions(text: str):
                results = []
                i = 0
                while i < len(text):
                    if text[i:i + 2] == "($":
                        start = i
                        depth = 1
                        i += 2
                        while i < len(text) and depth > 0:
                            if text[i:i + 2] == "($":
                                depth += 1
                                i += 2
                            elif text[i] == ")":
                                depth -= 1
                                i += 1
                            else:
                                i += 1
                        if depth == 0:
                            results.append(text[start:i])
                    else:
                        i += 1
                return results

            matches = extract_top_level_expressions(expression)

            result = expression
            for match in matches:
                value = SafeEvaluator.interpolate(match, merged_data=_merged_data)
                result = result.replace(match, str(value), 1)

            return result
