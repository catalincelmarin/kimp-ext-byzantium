import asyncio
import multiprocessing
import queue
import threading
import time
from typing import Any

import jmespath
from kimera.helpers.Helpers import Helpers
from kimera.process.ThreadKraken import ThreadKraken
from kimera.process.Spawner import Spawner

from .Stalker import Stalker
from .models import ControlMessage
from ..blackboard.SynodeBlackboard import SynodeBlackboard


class Argus:
    def __init__(self):
        """
        Argus only uses ThreadKraken to manage its own lightweight listener thread.
        Stalkers are started by the Spawner (as full subprocesses).
        """
        self.kraken = ThreadKraken()
        self.spawner = Spawner()      # for external stalkers as real processes
        self.running_stalkers = {}
        self._blackboard = None

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

    import jmespath

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
            result = jmespath.search(expression, data)
            return result
        except Exception as e:
            raise ValueError(f"Invalid jmespath expression: {e}")



    async def fanout(self,message):
        for name,stalker in self.running_stalkers.items():
            msg = ControlMessage(origin="argus",target=name,message=message).model_dump_json()
            stalker.control_queue.put(msg)

    async def notify(self,stalker: str,message):
        if stalker in self.running_stalkers:
            msg = ControlMessage(origin="argus",target=stalker,message=message).model_dump_json()
            self.running_stalkers[stalker].control_queue.put(msg)

    async def start_stalker(self, name, stalker_class, *args, **kwargs) -> Stalker:
        """
        Start a new stalker using the Spawner (not ThreadKraken!).
        """
        stalker = stalker_class(argus=self, name=name)

        def run_loop(stop: threading.Event, *args, **kwargs):
            _loop = asyncio.new_event_loop()
            async def looper():
                Helpers.infoPrint(f"{stalker.name} is publishing")
                while not stop.is_set():
                    await asyncio.sleep(1)
                    await stalker.feedback()
            _loop.run_until_complete(looper())
            Helpers.sigPrint("LOOP_CLOSING")
            _loop.close()

        if stalker.outbound:
            self.kraken.register_thread(name=f"fbk_{name}",target=run_loop)

        # Launch using Spawner, in a subprocess
        self.spawner.loop(name=name, coro=stalker.stalk, params={}, perpetual=True)

        # Keep a reference if needed
        self.running_stalkers[name] = stalker
        return stalker

    import time
    from .models import ControlMessage

    async def stop_stalker(self, name):
        """
        Politely send STOP to a stalker, then force kill if needed.
        """
        if name not in self.running_stalkers:
            print(f"[Argus] No stalker named {name} to stop.")
            return
        print(f"[Argus] Sent STOP signal to {name} threads.")
        self.running_stalkers[name].stop()
        print(f"[Argus] Sent STOP signal to {name} process.")

        # Step 2: Wait a bit for stalker to self-terminate
        await asyncio.sleep(5)  # adjust timeout if needed

        # Step 3: Force kill if still alive
        self.spawner.stop(name)
        if name in self.running_stalkers:

            del self.running_stalkers[name]

        print(f"[Argus] Stalker {name} stopped.")

    async def shutdown(self):
        """
        Graceful shutdown: stops all stalkers (processes) and listener (thread).
        """
        print("[Argus] Shutting down...")
        self.kraken.stop_all_threads()
        self.spawner.cleanup()          # stops all stalkers

    async def route_message(self,msg):
        print(msg)
        if msg.target in self.running_stalkers:
            self.running_stalkers[msg.target].control_queue.put(msg.model_dump_json())
        else:
            await self.handle_message(msg)

    async def handle_message(self, msg: ControlMessage):
        Helpers.sysPrint("HANDLING",msg.model_dump_json())


