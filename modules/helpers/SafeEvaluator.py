import re
import jmespath

class SafeEvaluator:
    _VAR_PATTERN = re.compile(r"\$([a-zA-Z0-9_.\[\]]+)")  # precompiled regex

    def __init__(self, blackboard=None):
        self._blackboard = blackboard
        self._data = blackboard if blackboard else {}

    def _resolve_var(self, match):
        var = match.group(1)
        value = jmespath.search(var, self._data.dump())

        if value is None:
            raise KeyError(f"Missing variable: ${var}")

        # Auto-stringify with quotes for strings
        return f'"{value}"' if isinstance(value, str) else str(value)

    def eval(self, expression):
        if not self._data:
            return expression

        expr = expression.strip()

        if expr.startswith("{") and expr.endswith("}"):
            inner = expr[1:-1]
            evaluated = self._VAR_PATTERN.sub(self._resolve_var, inner)
            return evaluated

        elif expr.startswith("$"):
            # Direct simple query
            key = expr.replace("$", "", 1)
            value = jmespath.search(key, self._data)

            if value is None:
                raise KeyError(f"Missing variable: ${key}")

            return value

        else:
            return expr