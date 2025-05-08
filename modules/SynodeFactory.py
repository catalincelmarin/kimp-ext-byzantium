import asyncio
import copy
import importlib
import os
import re
import time
from pathlib import Path
from typing import Dict, Any, Optional

import yaml
from kimera.helpers.Helpers import Helpers

from .blackboard.types.SynodeTypeLoader import SynodeTypeLoader
from .panoptes.Argus import Argus
from .panoptes.ArgusFactory import ArgusFactory
from .schematics.SynodeConfig import SynodeConfig
from .Synode import Synode, SynodeImpl



class SynodeFactory:
    _preloaded_configs: Dict[str, Dict[str, Any]] = {}
    _app_path = os.getenv("APP_PATH", "undefined")
    _locked = True


    def __init__(self, app_path):
        SynodeFactory._locked = False
        SynodeFactory._app_path = app_path


    @property
    def app_path(cls):
        return cls._app_path

    @staticmethod
    def load_config(yaml_path: str, alias: Optional[str] = None) -> Dict[str, Any]:
        if alias in SynodeFactory._preloaded_configs:
            return SynodeFactory._preloaded_configs[alias]

        if SynodeFactory._locked:
            raise Exception(f"use SynodeFactory.set_app_path to define the app_path where your application is running!")
        """
        Loads and registers a YAML config from file. Uses the 'name' field or an optional alias as the key.
        """
        if not yaml_path.startswith(SynodeFactory._app_path):

            full_path = os.path.join(SynodeFactory._app_path, yaml_path)
        else:
            full_path = yaml_path

        with open(full_path, 'r') as file:
            config = yaml.load(file,Loader=SynodeTypeLoader)

        name_in_config = config.get("name")
        if not name_in_config:
            raise ValueError(f"Missing 'name' in config file: {yaml_path}")

        name = alias or name_in_config

        if name in SynodeFactory._preloaded_configs:
            return SynodeFactory._preloaded_configs[name]

        SynodeFactory._preloaded_configs[name] = config
        return config



    @classmethod
    def synods_import(cls, folder_path: str):
        """
        Recursively loads all synod YAML configs from a folder and registers them by name.
        """
        full_folder = Path(os.path.join(cls._app_path, folder_path.replace(".", "/")))
        if not full_folder.is_dir():
            raise ValueError(f"Provided path '{folder_path}' is not a valid directory.")

        for file in full_folder.rglob("synod.*.yaml"):  # Changed to rglob for recursive search
            match = re.match(r"synod\.(?P<name>\w+)\.yaml", file.name)
            if match:
                try:
                    cls.load_config(str(file))
                    Helpers.infoPrint(f"Loaded config for: {match.group('name')}")
                except Exception as e:
                    Helpers.warnPrint(f"Skipping {file.name}: {e}")
            else:
                Helpers.warnPrint(f"Skipping {file.name}: filename doesn't match 'synod.[name].yaml' pattern.")

    @classmethod
    def summon(cls, name: str) -> Synode:
        if name not in cls._preloaded_configs:
            raise KeyError(f"No preloaded config found for name '{name}'.")

        raw_config = copy.deepcopy(cls._preloaded_configs[name])
        module_path = raw_config.get("module_class")
        klass = SynodeImpl  # Default fallback

        if module_path:
            try:
                mod_path, class_name = module_path.rsplit(".", 1)
                mod = importlib.import_module(module_path)
                imported = getattr(mod, class_name)

                if not issubclass(imported, Synode):
                    raise TypeError(f"{module_path} is not a subclass of Synode.")
                klass = imported

            except Exception as e:
                print(f"[WARN] Could not import '{module_path}', falling back to SynodeImpl: {e}")

        synode_config = SynodeConfig.from_config(raw_config, module_class=klass)
        return klass(synode_config)

    @classmethod
    def summon_argus(cls, synode_obj: Synode, argus_path) -> Argus:
        """
        Embed an Argus instance into a Synode.
        Hooks in the config must point to agent names (strings), not classes.
        The function for each hook will be dynamically created and passed to use_hook.
        """

        schema = ArgusFactory.load_schema_from_path(argus_path)

        base_module_path = argus_path  # Example: "app.ext.argus"

        # ✅ Prepare blackboard if defined
        blackboard_instance = None
        if schema.blackboard:
            blackboard_instance = synode_obj.blackboard

        # Load the main Argus class
        argus_class = ArgusFactory.load_class(f"{schema.module}.{schema.module.split('.')[-1]}")

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

            stalker_class = ArgusFactory.load_class(f"{stalker_module_path}.{stalker_class_name}")

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

        # Attach hooks from config

        for hook in schema.hooks:
            agent = hook.hook  # simple string, not path
            key_tuple = tuple(hook.keys)

            def make_hook(agent_name):

                time.sleep(5)
                async def handler(**kwargs):



                    await synode_obj.launch(trigger=agent_name, use_input=kwargs)

                return handler

            argus_instance.use_hook(list(key_tuple), make_hook(agent_name=agent))


        return argus_instance
