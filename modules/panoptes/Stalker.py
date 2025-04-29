import asyncio
import threading
from multiprocessing import Queue

from kimera.helpers.Helpers import Helpers
from kimera.process.ThreadKraken import ThreadKraken

from .models import ControlMessage



class Stalker:
    def __init__(self, argus, name):
        self.argus = argus
        self.control_queue = Queue()  # Each stalker owns its private queue
        self.name = name
        self.inbound = True
        self.outbound = True
        self._kraken = ThreadKraken()

    async def patch(self,message: ControlMessage):
        Helpers.sysPrint("PATCHER",message.model_dump_json())

    async def dispatch(self,message: ControlMessage):
        Helpers.print("dispatch")
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
            Helpers.infoPrint(f"{self.name} is listening")
            while not stop.is_set():
                await asyncio.sleep(1)
                await self.listen()

        _loop.run_until_complete(looper())
        _loop.close()


    async def stalk(self,*args,**kwargs):

        if self.inbound:
            self._kraken.register_thread(name=f"fbk_argus", target=self._run_loop)
            self._kraken.start_threads()

        Helpers.sysPrint("INBOUND")
        counter = 1
        while True:
            await asyncio.sleep(3)
            Helpers.sysPrint("looping",counter)
            if counter % 2 == 1:
                self.whisper(f"HELLO FROM BEHIND THE PROC {counter}")
            else:
                self.whisper(f"HELLO FROM BEHIND THE PROC {counter}",to="stalker2")
            counter += 1


    def whisper(self,message: str,to="argus"):
        Helpers.sysPrint(self.name,f"sending {message} to {to}")
        if to == self.name:
            print("kkt")
            return
        self.control_queue.put(ControlMessage(origin=self.name,target=to,message=message).model_dump_json())

    async def check_inbound_queue(self):
        if not self.control_queue.empty():
            raw_message = self.control_queue.get()
            try:
                msg = ControlMessage.model_validate_json(raw_message)
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
        """
        Local subprocess queue checking (for STOP or other direct control).
        """
        if not self.control_queue.empty():
            raw_message = self.control_queue.get()
            try:
                msg = ControlMessage.model_validate_json(raw_message)
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
            Helpers.sysPrint("PUBLISH",self.name)
            try:
                # Always pass incoming messages to the user-provided async callback
                await self.dispatch(message=msg)

            except Exception as e:
                print(f"[{self.name}] Invalid control message in feedback: {e}")
