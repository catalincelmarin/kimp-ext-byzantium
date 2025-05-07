import os
import importlib
import uuid

import yaml
from typing import Any, Optional

from celery.worker.control import heartbeat
from pydantic import ValidationError

from .ArgusSchema import ArgusSchema
from ..blackboard.Blackboard import Blackboard


class ArgusFactory:
    @staticmethod
    def load_schema_from_path(module_path: str) -> ArgusSchema:
        try:
            module = importlib.import_module(module_path)
        except ImportError as e:
            raise ImportError(f"Module '{module_path}' could not be imported.") from e

        if not hasattr(module, '__file__'):
            raise ValueError(f"Module '{module_path}' does not have a filesystem location.")

        module_dir = os.path.dirname(module.__file__)
        yaml_path = os.path.join(module_dir, 'panoptes.yaml')

        if not os.path.isfile(yaml_path):
            raise FileNotFoundError(f"No 'panoptes.yaml' found in {module_dir}")

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        try:
            return ArgusSchema(**data)
        except ValidationError as e:
            raise ValueError(f"Invalid YAML structure for ArgusSchema: {e}") from e

    @staticmethod
    def load_class(full_class_path: str) -> Any:
        parts = full_class_path.split(".")
        if len(parts) < 2:
            raise ValueError(f"Invalid class path '{full_class_path}'.")

        module_path = ".".join(parts[:-1])
        class_name = parts[-1]

        try:
            module = importlib.import_module(module_path)
        except ImportError as e:
            raise ImportError(f"Module '{module_path}' could not be imported.") from e

        if not hasattr(module, class_name):
            raise AttributeError(f"Module '{module_path}' does not contain class '{class_name}'.")

        return getattr(module, class_name)

    @classmethod
    def build(cls, module_path: str) -> Any:
        schema = cls.load_schema_from_path(module_path)

        base_module_path = module_path  # Example: "app.ext.argus"

        # ✅ Prepare blackboard if defined
        blackboard_instance = None
        if schema.blackboard:
            bb_class = schema.blackboard.blackboard_module  # Already resolved by validator
            bb_kwargs = schema.blackboard.kwargs or {}
            if bb_kwargs.get("namespace"):
                bb_kwargs["namespace"] += f"::{uuid.uuid4()[:6]}"
            blackboard_instance = bb_class(**bb_kwargs)


        # Load the main Argus class
        argus_class = cls.load_class(f"{schema.module}.{schema.module.split('.')[-1]}")

        # ✅ Pass blackboard into constructor
        argus_instance = argus_class(
            name=schema.name,
            blackboard=blackboard_instance,
            heartbeat=float(schema.heartbeat)
        )

        # Register stalkers
        for stalker_data in schema.stalkers:
            stalker_module_path = f"{base_module_path}.stalkers.{stalker_data.stalker}"
            stalker_class_name = stalker_data.stalker

            stalker_class = cls.load_class(f"{stalker_module_path}.{stalker_class_name}")

            argus_instance.register_stalker(
                name=stalker_data.name,
                stalker_class=stalker_class,
                heartbeat=float(stalker_data.heartbeat),
                startup=stalker_data.startup,
                **stalker_data.kwargs
            )

        # Register watches
        if hasattr(schema, "watch"):
            for watch_entry in schema.watch:
                argus_instance.use_watch(
                    key=watch_entry.key,
                    expression=watch_entry.expression,
                    default=watch_entry.default
                )

        # Register hooks
        if hasattr(schema, "hooks"):
            for hook_entry in schema.hooks:
                hook_path, method_name = hook_entry.hook.split("::")
                parts = hook_path.split(".")
                if len(parts) < 1:
                    raise ValueError(f"Invalid hook path: {hook_path}")

                module_path = ".".join(parts)
                class_name = parts[-1]

                try:
                    module = importlib.import_module(module_path)
                except ImportError as e:
                    raise ImportError(f"Failed to import module {module_path}") from e

                if not hasattr(module, class_name):
                    raise AttributeError(f"Module '{module_path}' does not contain class '{class_name}'.")

                klass = getattr(module, class_name)

                if not hasattr(klass, method_name):
                    raise AttributeError(f"Class '{class_name}' does not contain method '{method_name}'.")

                method = getattr(klass, method_name)

                argus_instance.use_hook(hook_entry.keys, method)

        return argus_instance


