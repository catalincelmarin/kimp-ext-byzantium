import asyncio
import pickle
import threading
from typing import Any, Optional, Dict, Callable

import jmespath
from kimera.helpers.Helpers import Helpers
from kimera.process.Spawner import Spawner
from kimera.process.ThreadKraken import ThreadKraken


from .Stalker import Stalker
from .models import ControlMessage
from ..blackboard.Blackboard import Blackboard


class Argus:
    def __init__(self,name,heartbeat: Optional[float] = 1,blackboard: Optional[Blackboard] = None):
        """
        Argus only uses ThreadKraken to manage its own lightweight listener thread.
        Stalkers are started by the Spawner (as full subprocesses).
        """
        self.name = name
        self._kraken = ThreadKraken()
        self._spawner = Spawner()      # for external stalkers as real processes
        self._running_stalkers = {}
        self._registered_stalkers = {}
        self._blackboard = blackboard
        self._mirror = {}
        self._hooks: Dict[tuple,Any] = {}
        self._watch: Dict[str,str] = {}
        self._watched = {}
        self._heartbeat = heartbeat
        self._is_running = False

    @property
    def is_running(self):
        return self._is_running

    @property
    def heartbeat(self):
        return self._heartbeat

    def use_hook(self, keys: list[str], method: Callable[..., Any]) -> None:
        """
        Registers a hook for a set of keys.
        Converts the list of keys into a tuple and stores the hook function.
        """
        key_tuple = tuple(keys)
        self._hooks[key_tuple] = method

    def use_watch(self, key: str, expression: str, default: Optional[Any] = None) -> None:
        """
        Registers a watch expression for a key.
        """
        self._watch[key] = expression
        self._mirror[key] = default

    @property
    def blackboard(self):
        """
        Getter for the blackboard.
        """
        return self._blackboard

    @blackboard.setter
    def blackboard(self, value):
        """
        Setter for the blackboard.
        """
        self._blackboard = value

    async def updateMirror(self, new_mirror: dict):
        changed_keys = []

        for key, new_value in new_mirror.items():
            old_value = self._mirror.get(key, None)
            if old_value != new_value:
                changed_keys.append(key)
                self._mirror[key] = new_value

        # Trigger hooks that depend on changed keys
        triggered = []

        for hook_keys, hook_callable in self._hooks.items():
            changed = [key for key in hook_keys if key in changed_keys]

            if changed:
                values = {key:new_mirror.get(key) for key in hook_keys}

                triggered.append(hook_callable(**values))

        await asyncio.gather(*triggered)

    async def query_blackboard(self, expression: str) -> Any:

        """
        Query the Argus blackboard using a jmespath expression.
        Returns the result of the query.

        Example expression: "synod_steller.*.directions"
        """
        if not self.blackboard:
            raise ValueError("Blackboard not initialized.")

        data = self.blackboard.dump()

        try:
            result = jmespath.search(expression.replace("$",""), data)
            return result
        except Exception as e:
            raise ValueError(f"Invalid jmespath expression: {e}")

    def overwatch(self, stop: threading.Event, *args, **kwargs):
            _loop = asyncio.new_event_loop()

            async def looper():
                Helpers.infoPrint(f"{self.name} is listening")
                while not stop.is_set():
                    await asyncio.sleep(self._heartbeat)
                    tasks = []
                    for exp in self._watch.values():
                        Helpers.print(self._blackboard.dump())
                        tasks.append(self.query_blackboard(exp))

                    results = await asyncio.gather(*tasks)

                    await self.updateMirror(dict(zip(list(self._watch.keys()),results)))

            _loop.run_until_complete(looper())
            _loop.close()

    async def fanout(self,message):
        for name,stalker in self._running_stalkers.items():
            msg = ControlMessage(origin="argus",target=name,message=message)
            stalker.control_queue.put(pickle.dumps(msg))

    async def notify(self,stalker: str,message):
        if stalker in self._running_stalkers:
            msg = ControlMessage(origin="argus",target=stalker,message=message)
            self._running_stalkers[stalker].control_queue.put(pickle.dumps(msg))

    def register_stalker(self,name, stalker_class,startup=True,*args, **kwargs):
        if name not in self._registered_stalkers:
            self._registered_stalkers[name] = stalker_class(argus=self, name=name,running=startup, **kwargs)

    def check_stalker(self, name):
        if name in self._registered_stalkers:
            return self._registered_stalkers[name].running

        return None

    async def run(self):
        if not self.is_running:
            for stalker in self._registered_stalkers.keys():
                if self._registered_stalkers.get(stalker).running:
                    await self.start_stalker(stalker)

            self._kraken.register_thread(name="argus",target=self.overwatch)
            self._kraken.start_thread("argus")
            self._is_running = True

        return self._is_running
    async def start_stalker(self, name, *args, **kwargs) -> Stalker | None:
        """
        Start a new stalker using the Spawner (not ThreadKraken!).
        """
        if name not in self._registered_stalkers:
            raise Exception(f"Stalker {name} not registered in argus:{self.name}")

        stalker = self._registered_stalkers[name]

        def run_loop(stop: threading.Event, *args, **kwargs):
            _loop = asyncio.new_event_loop()
            async def looper():
                Helpers.infoPrint(f"{stalker.name} is publishing")
                while not stop.is_set():
                    await asyncio.sleep(stalker.heartbeat)
                    await stalker.feedback()
            _loop.run_until_complete(looper())
            Helpers.sigPrint("LOOP_CLOSING")
            _loop.close()

        if stalker.outbound:
            self._kraken.register_thread(name=f"fbk_{name}",target=run_loop)
            self._kraken.start_thread(name=f"fbk_{name}")

        # Launch using Spawner, in a subprocess
        self._spawner.loop(name=name, coro=stalker.stalk, params={}, perpetual=True)

        # Keep a reference if needed
        self._running_stalkers[name] = stalker
        return stalker


    from .models import ControlMessage

    async def stop_stalker(self, name):
        """
        Politely send STOP to a stalker, then force kill if needed.
        """
        if name not in self._running_stalkers:
            print(f"[Argus] No stalker named {name} to stop.")
            return
        print(f"[Argus] Sent STOP signal to {name} threads.")
        self._running_stalkers[name].stop()
        print(f"[Argus] Sent STOP signal to {name} process.")

        # Step 2: Wait a bit for stalker to self-terminate
        await asyncio.sleep(5)  # adjust timeout if needed

        # Step 3: Force kill if still alive
        self._spawner.stop(name)
        if name in self._running_stalkers:

            del self._running_stalkers[name]

        print(f"[Argus] Stalker {name} stopped.")

    async def shutdown(self):
        """
        Graceful shutdown: stops all stalkers (processes) and listener (thread).
        """
        print("[Argus] Shutting down...")
        self._kraken.stop_all_threads()
        self._spawner.cleanup()
        self._is_running = False # stops all stalkers

    async def route_message(self,msg):
        if msg.target in self._running_stalkers:
            self._running_stalkers[msg.target].control_queue.put(msg.model_dump_json())
        else:
            await self.handle_message(msg)

    async def handle_message(self, msg: ControlMessage):
        Helpers.sysPrint("HANDLING",msg.message)



