import asyncio
import inspect

import re
from abc import ABC
from collections.abc import Callable
from typing import List, Dict, cast, Optional, Awaitable, Any, final, runtime_checkable, Protocol

import jmespath
from kimera.helpers.Helpers import Helpers

from kimera.process.TaskManager import TaskManager

from app.ext.byzantium.modules.SynodeBlackboard import BlackboardHandler
from app.ext.byzantium.modules.helpers.Helpers import Helpers as SynodeHelpers
from kimera.openai.gpt.BaseGPT import BaseGPT
from kimera.openai.gpt.BotFactory import BotFactory
from kimera.openai.gpt.chat import ChatMod
from kimera.openai.gpt.enums import ContentTypes, Roles

from app.src.bots.dxs.BaseHydra import BaseHydra


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
        from app.ext.byzantium.modules.schematics.SynodeConfig import SynodeConfig
        self.synode: SynodeConfig = config
        if self.synode.blackboard:
            self._blackboard = self.synode.blackboard()
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


    def done_callback(self,task):
        try:
            result = task.result()
            self._async_callback(result)
            print(f"Done with result: {result}")
        except Exception as e:
            print(f"Task failed: {e}")

    @final
    async def launch(self, trigger="main", use_input="", *args, **kwargs):
        result = None

        if self._async_callback:
            launcher = asyncio.create_task(self._run(trigger, use_input=use_input, *args, **kwargs))
            launcher.add_done_callback(self.done_callback)
        else:
            result = await self._run(trigger, use_input=use_input, *args, **kwargs)

        await self._clear_task_bucket()
        return result

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

    def _eval(self,expression):
        if not self._blackboard:
            return expression

        data = self._blackboard.dump()

        if expression.strip().startswith("{") and expression.strip().endswith("}"):
            variables = re.findall(r"\$([^\s}]+)", expression)

            resolved = {
                f"${var}": jmespath.search(var, data)
                for var in variables
            }

            for var_with_dollar, value in resolved.items():
                # Convert non-strings to string; quote strings
                if isinstance(value, str):
                    replacement = f'"{value}"'
                else:
                    replacement = str(value)

                # Replace *only* exact matches (not substrings inside words)
                expression = expression.replace(var_with_dollar, replacement)

            return expression.strip("{}")  # or eval(expression) if you're executing
        elif "$" in expression:

            return jmespath.search(expression.replace("$",""), data)
        else:
            return expression



    def _get_agent(self, agent):
        use_agent = self._eval(expression=agent)


        return next((item for item in self.synode.synode if item.agent == use_agent))

    @staticmethod
    async def run_bot(operator: BaseGPT,use_input,handler=None,instructions=None,content_type: ContentTypes = ContentTypes.TEXT):
        async with asyncio.Semaphore(30):
            extra = {}
            Helpers.sysPrint("OPERATOR TIMEOUT",operator.timeout)
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

            # Helpers.sysPrint("INPUT",use_input)

            result = await operator.chat([ChatMod(content=use_input,
                                             content_type=content_type,
                                             role=Roles.USER)],**extra)


            return result.content

    @staticmethod
    async def run_hydra(operator: BaseHydra, use_input, handler=None, instructions=None,
                      content_type: ContentTypes = ContentTypes.TEXT,*args, **kwargs):

        async with asyncio.Semaphore(30):
            extra = {}
            handler_name, selector = handler.split("@", 1) if "@" in handler else (handler, "auto")

            use_head: BaseGPT = operator.spawn(head_name=handler_name)
            # Helpers.sysPrint("HANDLER",handler)
            if instructions:
                use_input = f"INSTRUCTIONS: {instructions}\n INPUT: {use_input}"
                if selector not in ["stream","plain"]:
                    extra["call"] = selector
                elif selector == "stream":
                    extra["stream"] = use_head.stream


            # Helpers.sysPrint("INPUT", use_input)

            result = await use_head.chat(chat=[ChatMod(content=use_input,
                                                  content_type=content_type,
                                                  role=Roles.USER)], **extra)
            return result.content

    @staticmethod
    async def run_synode(operator: 'Synode', handler="main", use_input=None,instructions=None):

        return await operator.launch(trigger=handler,use_input=use_input,instructions=instructions)

    async def _apply_aop(self,coro: str, use_input, result=None):
        if hasattr(self, coro):
            method = getattr(self, coro)
            if callable(method):
                result = await method(use_input,result)  # Call the method
                return result
            else:
                print(f"Attribute '{method}' exists but is not callable.")
        else:
            print(f"Method '{coro}' not found on {self.__class__.__name__}.")

    @staticmethod
    async def run_basic(operator,handler: str, use_input, instructions=None):
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

    async def run_agent(self, agent: "SynodeAgent", use_input=None):

        from app.ext.byzantium.modules.schematics.SynodeConfig import SynodeOp,SynodeOpType,OperatorTypes

        if self._hook:
           self._task_bucket.append(asyncio.create_task(self._hook(self,action="enter", agent=agent,data=use_input)))
            

        if agent.before:
            use_input = await self._apply_aop(coro=agent.before,use_input=use_input)
            if self._hook:
               self._task_bucket.append(asyncio.create_task(self._hook(self,action="before", agent=agent,data=use_input)))

        parts = agent.operator.split("::")
        handler = parts[1] if len(parts) > 1 else None

        operator = self._operators[parts[0]]
        check_operator = self._get_list_operator(parts[0])

        agent_instructions = agent.instructions

        if self._blackboard:
            agent_instructions = self._eval(agent_instructions)

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
                    "instructions": agent_instructions
                },timeout=agent.timeout + 5)

            else:
                result = await self.run_hydra(operator=operator,
                                                handler=handler,
                                                use_input=use_input,
                                                instructions=agent_instructions,
                                                **agent.kwargs
                                               )

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

        elif check_operator.operator_type == OperatorTypes.BOT:

            if agent.run_async:
                # Helpers.sysPrint("dd",use_input)
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
                                            )

        elif check_operator.operator_type == OperatorTypes.BASIC:
            _kwargs = check_operator.kwargs.get(handler,{})
            _kwargs["use_input"] = use_input

            if agent.run_async:
                result = await TaskManager().send_await(task_name="run_basic", friend='byzantium', kwargs={
                    "operator": check_operator.model_dump(),
                    "agent": agent.model_dump(),
                    "handler": handler,
                    "use_input": _kwargs,
                    "instructions": agent_instructions,
                    "blackboard": BlackboardHandler(
                        handler=self.synode.blackboard.__module__,
                        data=self._blackboard.dump()
                    ).model_dump()
                },timeout=agent.timeout)
            else:
                result = await Synode.run_basic(operator=operator,handler=handler,use_input=_kwargs,instructions=agent_instructions)
        else:
            raise Exception("Wrong agent config (bot or synode only)")

        if self._blackboard and agent.store_key:
            self._blackboard.set(agent.store_key,result)

        if self._hook:
           self._task_bucket.append(asyncio.create_task(self._hook(self, action="output", agent=agent,data=result)))

        for _op in agent.operations:
            op: SynodeOp = cast(SynodeOp,_op)
            if self._hook:
               self._task_bucket.append(asyncio.create_task(self._hook(self, action="enter", operation=op, agent=agent, data=result)))
            if op.before:
                result = await self._apply_aop(coro=op.before, use_input=result)
                if self._hook:
                   self._task_bucket.append(asyncio.create_task(self._hook(self, action="before", operation=op, agent=agent, data=result)))

            if op.op_type == SynodeOpType.CHAIN_TO:
                # Helpers.print(self._blackboard.dump())
                target = self._get_agent(self._eval(op.target))
                if target:
                    result = await self.run_agent(agent=target, use_input=result)
                else:
                    raise Exception(f"{target} does not exist on {self.synode.name}")
            elif op.op_type == SynodeOpType.LOOP_TO:
                max_cycles = op.kwargs.get("max_cycles",1)
                _condition = op.kwargs.get("condition","`true`")

                _condition = self._eval(_condition)

                if agent.agent not in self._loops:
                    self._loops[agent.agent] = 0

                if self._loops[agent.agent] < max_cycles and _condition:
                    target = self._get_agent(op.target)

                    self._loops[agent.agent] += 1

                    result = await self.run_agent(agent=target, use_input=result)

            elif op.op_type == SynodeOpType.FORK_TO:
                task_list = []
                for target in op.target:
                    use_target = self._get_agent(target)
                    task_list.append(asyncio.create_task(self.run_agent(agent=use_target, use_input=result)))

                result = await asyncio.gather(*task_list)
            elif op.op_type == SynodeOpType.MAP or op.op_type == SynodeOpType.FILTER:
                if not isinstance(result,list):
                    raise Exception("MAP requires an array")


                task_list = []
                for part in result:
                    use_target = self._get_agent(op.target)
                    task_list.append(asyncio.create_task(self.run_agent(agent=use_target, use_input=part)))

                    result = await asyncio.gather(*task_list)

                if op.op_type == SynodeOpType.FILTER:
                    result = [part for part in result if part not in (False, None)]

            elif op.op_type == SynodeOpType.REDUCE:
                if not isinstance(result,list):
                    raise Exception("REDUCE requires an array")
                accumulate = op.kwargs.get("accumulator")
                for part in result:
                    use_target = self._get_agent(op.target)
                    accumulate = await self.run_agent(agent=use_target, use_input={"accumulate": accumulate,"item": part})

                result = accumulate



            if self._hook:
               self._task_bucket.append(asyncio.create_task(self._hook(self, action="result", operation=op, agent=agent, data=result)))
            if op.after:
                result = await self._apply_aop(coro=op.after, use_input=use_input,result=result)
                if self._hook:
                   self._task_bucket.append(asyncio.create_task(self._hook(self, action="after", operation=op, agent=agent, data=result)))


        if self._hook:
           self._task_bucket.append(asyncio.create_task(self._hook(self, action="exit", agent=agent,data=result)))
        if agent.after:
            result = await self._apply_aop(coro=agent.after,use_input=use_input,result=result)
            if self._hook:
               self._task_bucket.append(asyncio.create_task(self._hook(self, action="after", agent=agent,data=result)))

        return result


    def _load_operators(self):
        from app.ext.byzantium.modules.SynodeFactory import SynodeFactory
        from app.ext.byzantium.modules.schematics.SynodeConfig import OperatorTypes

        for operator in self.synode.operators:
            if operator.operator_type == OperatorTypes.HYDRA:
                hydra = cast(BaseHydra,BotFactory.summon(bot_name=operator.operator_path))
                for head, definition in operator.handlers.items():
                    hydra.spawn(head_name=head,**definition.kwargs)

                self._operators[operator.alias] = hydra

            if operator.operator_type == OperatorTypes.BOT:
                self._operators[operator.alias] = BotFactory.summon(bot_name=operator.operator_path)

            elif operator.operator_type == OperatorTypes.SYNOD:
                SynodeFactory.load_config(operator.operator_path, alias=operator.alias)
                self._operators[operator.alias] = SynodeFactory.summon(operator.alias)

            elif operator.operator_type == OperatorTypes.BASIC:
                klass = SynodeHelpers.get_class(operator.operator_path)

                try:
                    _kwargs = operator.kwargs.get("constructor",{})

                    signature = inspect.signature(klass.__init__)

                    # Extract parameter names, excluding 'self'
                    param_names = [param.name for param in signature.parameters.values() if param.name != 'self']

                    if self._blackboard and "blackboard" in param_names:
                        self._operators[operator.alias] = klass(blackboard=self._blackboard, **_kwargs)
                    else:
                        self._operators[operator.alias] = klass(**_kwargs)

                except Exception as e:
                    Helpers.errPrint(e,"Synode.py",171)

    async def _run(self,trigger="main", use_input=None, instructions=None, *args, **kwargs):

        trigger_agent = next((item for item in self.synode.synode if item.agent == trigger), None)

        if trigger_agent:
            if self._hook:
                await self._hook(self, action="launch", agent=trigger_agent,data=use_input)

            data = await self.run_agent(trigger_agent,use_input)
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
