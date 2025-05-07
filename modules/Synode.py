import asyncio
import inspect

import uuid
from abc import ABC
from typing import Dict, cast, Optional, Any, final, runtime_checkable, Protocol


from kimera.helpers.Helpers import Helpers
from kimera.openai.gpt.BaseHydra import BaseHydra

from kimera.process.TaskManager import TaskManager

from .panoptes.Argus import Argus

from .blackboard.Blackboard import BlackboardHandler
from .blackboard.InMemoryBlackboard import InMemoryBlackboard
from .helpers.Helpers import Helpers as SynodeHelpers
from kimera.openai.gpt.BaseGPT import BaseGPT
from kimera.openai.gpt.BotFactory import BotFactory
from kimera.openai.gpt.chat import ChatMod
from kimera.openai.gpt.enums import ContentTypes, Roles

from .helpers.SafeEvaluator import SafeEvaluator
from .schematics.Enums import Signals
from .wrappers.SynodeHydra import SynodeHydra


@runtime_checkable
class HookCallable(Protocol):
    async def __call__(_,
                       self: "Synode" ,
                       action: str,
                       data: Any,
                       operation: Optional["SynodeOp"] = None,
                       agent: Optional["SynodeAgent"] = None,
                       *args: Any,
                       **kwargs: Any) -> Any:
            ...

class Synode(ABC):

    def __init__(self, config):
        from .schematics.SynodeConfig import SynodeConfig
        self.synode: SynodeConfig = config
        if self.synode.blackboard:
            bb_kwargs = self.synode.blackboard.kwargs
            if bb_kwargs.get("namespace") and not self.synode.persistent:
                bb_kwargs["namespace"] += f"::{uuid.uuid4()}"

            self._blackboard = self.synode.blackboard.blackboard_module(**bb_kwargs)


        else:
            self._blackboard = None

        self._async_callback = None
        if self.synode.async_callback:
            self._async_callback = SynodeHelpers.get_method(self,self.synode.async_callback)

        self._operators: Dict[str,Any] = {}
        self._load_operators()
        self._main_input = None
        self._loops: Dict[str,int] = {}
        self._hook: Optional[HookCallable] = self.__hook__
        self._task_bucket = []
        self._evaluator = SafeEvaluator(self._blackboard)
        self._init_blackboard()


    def _init_private_board(self,private_board: InMemoryBlackboard,session=None):
        if not session:
            session = {}

        for key, value in session.items():
            private_board.set(key,value)

        for agent in self.synode.synode:
            if agent.store_key and agent.store_key.startswith("_"):
                private_board.set(agent.store_key, agent.default_value)

            for op in agent.operations:
                if op.store_key and op.store_key.startswith("_"):
                    private_board.set(op.store_key,op.default_value)

    def _init_blackboard(self):
        """
        Initializes the blackboard by setting store_key/default_value
        found in the agents and operations inside self.synode config.
        """
        if not self._blackboard:
            return

        for agent in self.synode.synode:
            if agent.store_key and not agent.store_key.startswith("_"):
                use_value = self._blackboard.get(agent.store_key)
                self._blackboard.set(agent.store_key, use_value if use_value is not None else agent.default_value)

            for op in agent.operations:
                if op.store_key and not op.store_key.startswith("_"):
                    use_value = self._blackboard.get(agent.store_key)
                    self._blackboard.set(op.store_key, use_value if use_value is not None else op.default_value)

    def set_streamer(self,operator_name,streamer):
        op = self._operators.get(operator_name,None)
        if isinstance(op,BaseGPT) or isinstance(op,BaseHydra):
            op.streamer = streamer
            if isinstance(op,BaseHydra):
                for head in op.heads.values():
                    head.streamer = streamer
        else:
            raise Exception(f"Operator {operator_name} does not support streaming")

    def _get_list_operator(self,alias):
        return next((item for item in self.synode.operators if item.alias == alias))

    async def _clear_task_bucket(self):
        for task in self._task_bucket:
            if not task.done():
                task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                # Swallow it—expected during shutdown
                pass
            except Exception as e:
                # Log or handle unexpected exceptions
                print(f"[Cleanup] Unexpected exception: {e}")
        self._task_bucket.clear()

    def _add_operator(self,operator: 'SynodeOperator'):
        self.synode.operators.append(operator)
        self._set_operator(operator=operator)

    def done_callback(self,task):
        try:
            result = task.result()
            self._async_callback(result)
            print(f"Done with result: {result}")
        except Exception as e:
            print(f"Task failed: {e}")

    @final
    async def launch(self, trigger="main", use_input="",session=None, *args, **kwargs):

        private_board = InMemoryBlackboard()
        if isinstance(session,dict):
            self._init_private_board(private_board=private_board,session=session)

        result = await self._run(trigger, use_input=use_input,private_board=private_board, *args, **kwargs)

        if not self.synode.persistent and self.blackboard:
            Helpers.sysPrint("IS NOT PERSISTENT",self.synode.name)
            self.blackboard.clear()

        private_board.clear()
        del private_board

        await self._clear_task_bucket()
        return result

    @final
    def start(self,trigger="main", use_input="",session=None, *args, **kwargs):
        if self.synode.daemon:
            try:
                _loop = asyncio.get_running_loop()
                # If we're already in a loop, just create and schedule the task
                _loop.create_task(self.launch(trigger=trigger, use_input="",session=session, *args, **kwargs))
            except RuntimeError:
                # No loop is running; create one and run the task
                _loop = asyncio.new_event_loop()
                asyncio.set_event_loop(_loop)
                _run_task = _loop.create_task(self.launch(trigger=trigger, use_input="",session=session, *args, **kwargs))
                _loop.run_forever()
        else:
            asyncio.run(self.launch(trigger=trigger,use_input="",session=session,*args,**kwargs))
            self.blackboard.clear()



    @property
    def hook(self) -> Optional[HookCallable]:
        return self._hook

    @hook.setter
    def hook(self, coro: Optional[HookCallable]) -> None:
        if coro is not None and not isinstance(coro, HookCallable):
            raise TypeError("hook must match the HookCallable protocol")
        self._hook = coro

    @property
    def blackboard(self):
        return self._blackboard

    def _get_agent(self, agent, private_board: Optional[InMemoryBlackboard]=None):
        if not private_board:
            private_board = None

        use_agent = self._evaluator.eval(expression=agent,private_board=private_board)


        return next((item for item in self.synode.synode if item.agent == use_agent))

    @staticmethod
    async def run_bot(operator: BaseGPT,use_input,handler=None,instructions=None,content_type: str = "TEXT",*args,**kwargs):
        async with asyncio.Semaphore(30):
            extra = {"session":kwargs.get("session",{})}

            if instructions:
                use_input = f"INSTRUCTIONS: {instructions}\n INPUT: {use_input}"
            if hasattr(operator,"tools") and handler in operator.tools:
                tool = SynodeHelpers.get_method(operator,handler)
                extra["call"] = tool
            elif handler in operator.response_formats:
                extra["response_format"] = handler
            elif handler in ["auto","required"]:
                extra["call"] = handler
            elif handler == "stream":
                extra["stream"] = operator.stream



            result = await operator.chat([ChatMod(content=use_input,
                                             content_type=ContentTypes[content_type].value,
                                             role=Roles.USER)],**extra)


            return result.content

    @staticmethod
    async def run_hydra(operator: BaseHydra, use_input, handler=None, instructions=None,
                      content_type: str = "TEXT",*args, **kwargs):

        async with asyncio.Semaphore(30):


            extra = {"session": kwargs.get("session",{"test":120})}
            handler_name, selector = handler.split("@", 1) if "@" in handler else (handler, "auto")

            use_head: BaseGPT = operator.spawn(head_name=handler_name)

            if instructions:
                use_input = f"INSTRUCTIONS: {instructions}\n INPUT: {use_input}"
                if selector not in ["stream","plain"]:
                    Helpers.sysPrint("EXTRA CALL",selector)
                    extra["call"] = selector
                elif selector == "stream":
                    extra["stream"] = use_head.stream

            c_type =ContentTypes[content_type].value
            use_head.flush()
            result = await use_head.chat(chat=[ChatMod(content=use_input,
                                                  content_type=c_type,
                                                  role=Roles.USER)], **extra)


            return result.content

    @staticmethod
    async def run_synode(operator: 'Synode', handler="main", use_input=None,instructions=None):

        return await operator.launch(trigger=handler,use_input=use_input,instructions=instructions)

    @staticmethod
    async def run_argus(operator: Argus, handler="main", use_input=None, instructions=None):
        handler_name, selector = handler.split("@", 1) if "@" in handler else (handler, "main")
        if selector == "start":
            if handler_name == "main":
                argus = await operator.run()
            elif operator.check_stalker(handler_name) is not None:


                if not operator.check_stalker(handler_name):
                    await operator.start_stalker(handler_name)
        elif selector == "stop":
            if handler_name == "main":
                argus = await operator.shutdown()
            elif operator.check_stalker(handler_name) is not None:
                if operator.check_stalker(handler_name):

                    await operator.stop_stalker(handler_name)

        return True

    async def _apply_aop(self,coro: str, use_input, result=None,session: Optional[InMemoryBlackboard] = None,*args,**kwargs):
        pass_session = {}
        if session:
            pass_session = session.dump()

        if hasattr(self, coro):
            method = getattr(self, coro)
            if callable(method):
                result = await method(use_input=use_input,result=result,session=pass_session,*args,**kwargs)  # Call the method
                return result
            else:
                print(f"Attribute '{method}' exists but is not callable.")
        else:
            print(f"Method '{coro}' not found on {self.__class__.__name__}.")

    @staticmethod
    async def run_basic(operator,handler: str, use_input, instructions=None, *args, **kwargs):
        async with asyncio.Semaphore(100):
            if hasattr(operator, handler):
                method = getattr(operator, handler)
                if callable(method):
                    result = await method(**use_input)  # Call the method
                    return result
                else:
                    print(f"Attribute '{method}' exists but is not callable.")
            else:
                print(f"Method '{handler}' not found on {operator.__class__.__name__}.")

    async def run_agent(self, agent: "SynodeAgent", use_input=None, *args, **kwargs):

        from .schematics.SynodeConfig import SynodeOp,SynodeOpType,OperatorTypes
        private_board: Optional[InMemoryBlackboard] = None

        if kwargs.get("private_board",None):
           private_board: InMemoryBlackboard = kwargs.get("private_board")

        if self._hook:
           self._task_bucket.append(asyncio.create_task(self._hook(self,action="enter", agent=agent,data=use_input)))
            

        if agent.before:
            use_input = await self._apply_aop(coro=agent.before,use_input=use_input,session=private_board)
            if self._hook:
               self._task_bucket.append(asyncio.create_task(self._hook(self,action="before", agent=agent,data=use_input)))

        if isinstance(use_input,Signals):
                return  use_input

        use_operator = self._evaluator.eval(agent.operator.replace("@","\n"),private_board=private_board).replace("\n","@")

        parts = use_operator.split("::")
        handler = parts[1] if len(parts) > 1 else None

        operator = self._operators[parts[0]]
        check_operator = self._get_list_operator(parts[0])

        agent_instructions = agent.instructions

        if self._blackboard:
            agent_instructions = self._evaluator.eval(agent_instructions,private_board=private_board)

        operator.timeout = agent.timeout
        if check_operator.operator_type == OperatorTypes.HYDRA:
            if not handler:
                handler = "main"

            if agent.run_async:
                result = await TaskManager().send_await(task_name="run_hydra", friend='byzantium', kwargs={
                    "operator": check_operator.model_dump(),
                    "agent": agent.model_dump(),
                    "handler": handler,
                    "use_input": use_input,
                    "instructions": agent_instructions,
                    **agent.kwargs
                },timeout=agent.timeout + 5)

            else:
                Helpers.sysPrint("PRIVATE","board")
                Helpers.print(private_board.dump())
                result = await self.run_hydra(operator=operator,
                                                handler=handler,
                                                use_input=use_input,
                                                instructions=agent_instructions,
                                                session=private_board.dump(),
                                                **agent.kwargs
                                               )

            if isinstance(result,dict) and result.get("sys_prompt",None):
                from .schematics.SynodeConfig import OperatorHandler
                new_head = result

                head_name = new_head.get("head_name",uuid.uuid4().hex[:8])
                head_kwargs = {
                    "sys_prompt": f"HEAD NAME: [{head_name}] " + new_head.get("sys_prompt"),
                    "tools": new_head.get("tools", []),
                    "description": new_head.get("instructions", "spawned head"),
                }

                h_handler = OperatorHandler(kwargs=head_kwargs)

                check_operator.handlers[head_name] = h_handler
                operator.spawn(head_name=new_head.get("head_name"),**h_handler.kwargs)

                result = {
                    "head_name":head_name,
                    "instructions": new_head.get("instructions"),
                }


        elif check_operator.operator_type == OperatorTypes.SYNOD:
            if not handler:
                handler = "main"

            if agent.run_async:
                result = await TaskManager().send_await(task_name="run_synode", friend='byzantium', kwargs={
                    "operator": check_operator.model_dump(),
                    "agent": agent.model_dump(),
                    "handler": handler,
                    "use_input": use_input,
                    "instructions": agent_instructions
                },timeout=agent.timeout + 5)

            else:
                result = await self.run_synode(operator=operator,
                                                handler=handler,
                                                use_input=use_input,
                                                instructions=agent_instructions)

        elif check_operator.operator_type == OperatorTypes.ARGUS:
            if not handler:
                handler = "main"


            result = await self.run_argus(operator=operator,
                                            handler=handler,
                                            use_input=use_input,
                                            instructions=agent_instructions)

        elif check_operator.operator_type == OperatorTypes.BOT:

            if agent.run_async:
                #
                result = await TaskManager().send_await(task_name="run_bot", friend='byzantium', kwargs={
                    "operator": check_operator.model_dump(),
                    "agent": agent.model_dump(),
                    "handler": handler,
                    "use_input": use_input,
                    "instructions": agent_instructions
                },timeout=agent.timeout + 5)
            else:
                result = await self.run_bot(operator=operator,
                                             handler=handler,
                                             use_input=use_input,
                                             instructions=agent_instructions,
                                             session=private_board.dump()
                                            )

        elif check_operator.operator_type == OperatorTypes.BASIC:
            _kwargs = check_operator.kwargs.get(handler,{})
            _kwargs["use_input"] = use_input
            _kwargs = {**_kwargs,**kwargs}
            if agent.run_async:
                result = await TaskManager().send_await(task_name="run_basic", friend='byzantium', kwargs={
                    "operator": check_operator.model_dump(),
                    "agent": agent.model_dump(),
                    "handler": handler,
                    "use_input": _kwargs,
                    "instructions": agent_instructions,
                    "blackboard": BlackboardHandler(
                        handler=self.synode.blackboard.__module__,
                        data=self._blackboard.full_dump()
                    ).model_dump()
                },timeout=agent.timeout)
            else:
                result = await Synode.run_basic(operator=operator,handler=handler,use_input=_kwargs,instructions=agent_instructions,*args,**kwargs)
        else:
            raise Exception("Wrong agent config (bot or synode only)")

        if self._blackboard and agent.store_key:
            if agent.store_key.startswith("_"):
                private_board.set(agent.store_key,result)
            else:
                self._blackboard.set(agent.store_key,result)


        if self._hook:
           self._task_bucket.append(asyncio.create_task(self._hook(self, action="output", agent=agent,data=result)))

        if isinstance(result,Signals):
                return  use_input

        for _op in agent.operations:
            op: SynodeOp = cast(SynodeOp,_op)
            if self._hook:
               self._task_bucket.append(asyncio.create_task(self._hook(self, action="enter", operation=op, agent=agent, data=result)))
            if op.before:
                result = await self._apply_aop(coro=op.before, use_input=result,session=private_board)
                if self._hook:
                   self._task_bucket.append(asyncio.create_task(self._hook(self, action="before", operation=op, agent=agent, data=result)))

            if isinstance(use_input, Signals):
                return use_input

            if op.op_type == SynodeOpType.CHAIN_TO:
                Helpers.print(private_board.dump())
                target = self._get_agent(self._evaluator.eval(op.target,private_board=private_board))
                if target:
                    print({**kwargs,**op.kwargs})
                    result = await self.run_agent(agent=target, use_input=result, *args, **{**kwargs,**op.kwargs})
                else:
                    raise Exception(f"{target} does not exist on {self.synode.name}")
            elif op.op_type == SynodeOpType.LOOP_TO:
                max_cycles = op.kwargs.get("max_cycles",1)
                _condition = op.kwargs.get("condition","`true`")

                _condition = self._evaluator.eval(_condition,private_board=private_board)

                if agent.agent not in self._loops:
                    self._loops[agent.agent] = 0

                if self._loops[agent.agent] < max_cycles and _condition:
                    target = self._get_agent(op.target,private_board=private_board)

                    self._loops[agent.agent] += 1

                    result = await self.run_agent(agent=target, use_input=result, *args, **kwargs)

            elif op.op_type == SynodeOpType.FORK_TO:
                task_list = []
                for target in op.target:
                    use_target = self._get_agent(target,private_board=private_board)

                    task_list.append(asyncio.create_task(self.run_agent(agent=use_target, use_input=result, *args, **kwargs)))

                result = await asyncio.gather(*task_list)
            elif op.op_type == SynodeOpType.MAP or op.op_type == SynodeOpType.FILTER:
                if not isinstance(result,list):
                    raise Exception("MAP requires an array")


                task_list = []
                for part in result:
                    use_target = self._get_agent(op.target,private_board=private_board)
                    task_list.append(asyncio.create_task(self.run_agent(agent=use_target, use_input=part, *args, **kwargs)))

                    result = await asyncio.gather(*task_list)

                if op.op_type == SynodeOpType.FILTER:
                    result = [part for part in result if part not in (False, None)]

            elif op.op_type == SynodeOpType.REDUCE:
                if not isinstance(result,list):
                    raise Exception("REDUCE requires an array")
                accumulate = op.kwargs.get("accumulator")
                for part in result:
                    use_target = self._get_agent(op.target,private_board=private_board)
                    accumulate = await self.run_agent(agent=use_target, use_input={"accumulate": accumulate,"item": part}, *args, **kwargs)

                result = accumulate

            if isinstance(result, Signals):
                return use_input

            if self._blackboard and op.store_key:
                if op.store_key.startswith("_"):
                    private_board.set(op.store_key, result)
                else:
                    self._blackboard.set(op.store_key, result)

            if self._hook:
               self._task_bucket.append(asyncio.create_task(self._hook(self, action="result", operation=op, agent=agent, data=result)))
            if op.after:
                result = await self._apply_aop(coro=op.after, use_input=use_input,result=result,session=private_board)
                if self._hook:
                   self._task_bucket.append(asyncio.create_task(self._hook(self, action="after", operation=op, agent=agent, data=result)))

            if isinstance(result, Signals):
                return use_input

        if self._hook:
           self._task_bucket.append(asyncio.create_task(self._hook(self, action="exit", agent=agent,data=result)))
        if agent.after:
            result = await self._apply_aop(coro=agent.after,use_input=use_input,result=result,session=private_board)
            if self._hook:
               self._task_bucket.append(asyncio.create_task(self._hook(self, action="after", agent=agent,data=result)))

        return result

    def _load_operators(self):

        for operator in self.synode.operators:
            self._set_operator(operator)

    def _set_operator(self, operator):
        from .SynodeFactory import SynodeFactory
        from .schematics.SynodeConfig import OperatorTypes

        if operator.operator_type == OperatorTypes.ARGUS:
            argus = SynodeFactory.summon_argus(self,argus_path=operator.operator_path)

            self._operators[operator.alias] = argus


        if operator.operator_type == OperatorTypes.HYDRA:
            hydra = cast(SynodeHydra, BotFactory.summon(bot_name=operator.operator_path))
            #hydra.blackboard = self.blackboard
            for head, definition in operator.handlers.items():
                hydra.spawn(head_name=head, **definition.kwargs)


            self._operators[operator.alias] = hydra

        elif operator.operator_type == OperatorTypes.BOT:
            self._operators[operator.alias] = BotFactory.summon(bot_name=operator.operator_path)
            #self._operators[operator.alias].blackboard = self._blackboard

        elif operator.operator_type == OperatorTypes.SYNOD:
            SynodeFactory.load_config(operator.operator_path, alias=operator.alias)
            self._operators[operator.alias] = SynodeFactory.summon(operator.alias)

        elif operator.operator_type == OperatorTypes.BASIC:
            klass = SynodeHelpers.get_class(operator.operator_path)
            try:
                _kwargs = operator.kwargs.get("constructor", {})
                signature = inspect.signature(klass.__init__)
                param_names = [param.name for param in signature.parameters.values() if param.name != 'self']

                if self._blackboard and "blackboard" in param_names:
                    self._operators[operator.alias] = klass(blackboard=self._blackboard, **_kwargs)
                else:
                    self._operators[operator.alias] = klass(**_kwargs)

            except Exception as e:
                Helpers.errPrint(e, "Synode.py", 171)

    async def _run(self,trigger="main", use_input=None,private_board=None, instructions=None, *args, **kwargs):

        trigger_agent = next((item for item in self.synode.synode if item.agent == trigger), None)

        if trigger_agent:
            if self._hook:
                await self._hook(self, action="launch", agent=trigger_agent,data=use_input)

            data = await self.run_agent(trigger_agent,use_input=use_input,private_board=private_board)
            self._loops = {}
            return data
        else:
            raise Exception(f"Agent {trigger_agent} does not exist")


    async def __hook__(_,self: "Synode", action, data, operation: Optional["SynodeOp"] = None, agent: Optional["SynodeAgent"] = None, *args,
                       **kwargs):

        """
            this method can be overwritten by any other callback that must respect this signature
            it provides a hook functionality action can have values like "before" or "after" it allowd dispatching
            it does not interfere with the data and it must not alter any reference
            it's purpose is only logging and monitoring and debugging,
            for AOP injections use provided before and after hook keys on agents and operations
            !!! this is the reference to current Synode,
        """
        if action == "launch":
            Helpers.sysPrint("LAUNCHING AGENT",self.synode.name)
        elif operation and agent:
            Helpers.sysPrint("OPERATION", f"{agent.agent}::{action}::{operation.op_type}")
        elif agent:
            Helpers.sysPrint("AGENT", f"{action}::{agent.agent}")


class SynodeImpl(Synode):
    pass
