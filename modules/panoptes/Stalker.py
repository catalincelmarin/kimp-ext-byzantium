import asyncio
import pickle
import threading
from abc import abstractmethod
from multiprocessing import Queue
from typing import Optional, Any

from kimera.helpers.Helpers import Helpers
from kimera.process.ThreadKraken import ThreadKraken

from .models import ControlMessage
from ..blackboard.Blackboard import Blackboard


class Stalker:
    def __init__(self, argus, name,running,heartbeat: Optional[float] = 1, inbound=False, outbound=False):
        self.argus = argus
        self._control_queue = Queue()  # Each stalker owns its private queue
        self.name = name
        self._inbound = inbound
        self._running = running
        self._outbound = outbound
        self._blackboard = self.argus.blackboard
        self._kraken = ThreadKraken()

        self._heartbeat = heartbeat

    @property
    def running(self):
        Helpers.sysPrint("GET RUNNING",self._running)
        return self._running

    @running.setter
    def running(self,value: bool):
        Helpers.sysPrint("SET RUNNING", self._running)
        self._running = value

    @property
    def heartbeat(self):
        return self._heartbeat

    @property
    def control_queue(self):
        return self._control_queue

    @property
    def inbound(self):
        return self._inbound

    @property
    def outbound(self):
        return self._outbound

    @property
    def blackboard(self) -> Blackboard | None:
        return self._blackboard

    @abstractmethod
    async def patch(self,message: ControlMessage):
        pass

    async def dispatch(self,message: ControlMessage):

        await self.argus.route_message(message)

    async def listen(self):
        """
        The stalker's core work loop — runs inside subprocess.
        """
        try:
            msg = await self.check_inbound_queue()
            if msg is not None:
                await self.patch(msg)

        except asyncio.CancelledError:
            print(f"[{self.name}] Stalker loop cancelled cleanly.")

    def stop(self):
        self._kraken.stop_all_threads()

    def _run_loop(self,stop: threading.Event, *args, **kwargs):
        _loop = asyncio.new_event_loop()

        async def looper():
            #Helpers.infoPrint(f"{self.name} is listening")
            while not stop.is_set():
                await asyncio.sleep(self._heartbeat)
                await self.listen()

        _loop.run_until_complete(looper())
        _loop.close()

    @abstractmethod
    async def execute(self):
        pass

    async def stalk(self,stop: threading.Event=None,*args,**kwargs):
        Helpers.sysPrint("STALKING",self.name)
        if self.inbound:
            self._kraken.register_thread(name=f"fbk_argus", target=self._run_loop)
            self._kraken.start_threads()

        Helpers.sysPrint("HEARTBEAT",self._heartbeat)
        while True:
            await self.execute()
            await asyncio.sleep(self._heartbeat)



    def whisper(self, message: Any, to="argus"):
        if to == self.name:
            return
        msg = ControlMessage(origin=self.name, target=to, message=message)
        self.control_queue.put(pickle.dumps(msg))  # ✅ Pickle before putting

    async def check_inbound_queue(self):
        if not self.control_queue.empty():
            raw_message = self.control_queue.get()
            try:
                msg = pickle.loads(raw_message)  # ✅ Unpickle directly

                if not isinstance(msg, ControlMessage):
                    raise TypeError("Received object is not a ControlMessage")

                if msg.target == self.name:
                    if msg.message == "STOP":
                        print(f"[{self.name}] Received STOP. Exiting stalker.")
                        raise asyncio.CancelledError()
                    else:
                        return msg
                else:
                    self.control_queue.put(raw_message)

            except Exception as e:
                print(f"[{self.name}] Invalid control message: {e}")

        return None

    async def check_outbound_queue(self):
        if not self.control_queue.empty():
            raw_message = self.control_queue.get()
            try:
                msg = pickle.loads(raw_message)  # ✅ Unpickle

                if not isinstance(msg, ControlMessage):
                    raise TypeError("Invalid message type")

                if msg.target != self.name:
                    return msg

            except Exception as e:
                print(f"[{self.name}] Invalid control message: {e}")

        return None

    async def feedback(self):
        """
        OPTIONAL external listening — runs in Argus main thread if wanted.
        Receives and processes feedback sent from the stalker's subprocess.
        """
        msg = await self.check_outbound_queue()
        if msg:
            # Helpers.sysPrint("PUBLISH",self.name)
            try:
                # Always pass incoming messages to the user-provided async callback
                await self.dispatch(message=msg)

            except Exception as e:
                print(f"[{self.name}] Invalid control message in feedback: {e}")
