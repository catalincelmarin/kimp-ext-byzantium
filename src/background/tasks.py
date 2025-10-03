import asyncio
import inspect
from typing import cast

from kimera.Bootstrap import Bootstrap
from kimera.helpers.Helpers import Helpers
from app.ext.kimeraai.src.kimllm.gpt.BaseHydra import BaseHydra
from app.ext.kimeraai.src.kimllm.gpt.BotFactory import BotFactory
from kimera.process.TaskManager import TaskManager

from ..synode.Synode import Synode
from ..synode.blackboard.Blackboard import BlackboardHandler
from ..synode.SynodeFactory import SynodeFactory
from ..synode.helpers.Helpers import Helpers as SynodeHelpers
from ..synode.schematics.SynodeConfig import SynodeAgent, Operator


boot = Bootstrap()
tm_app = TaskManager().celery_app
SynodeFactory(app_path=boot.root_path)



if tm_app:

    tm_app.conf.update({
        "broker_connection_retry_on_startup": True
    })

    @tm_app.task(queue="byzantium_q")
    def run_bot(
            operator: dict,
            agent: dict,
            handler: str,
            use_input: str,
            instructions: str,
            content_type: str = "TEXT"
    ):

        use_operator = Operator(**operator)
        use_agent = SynodeAgent(**agent)

        bot = BotFactory.summon(bot_name=use_operator.operator_path)
        bot.timeout = use_agent.timeout
        return asyncio.run(Synode.run_bot(operator=bot, use_input=use_input, handler=handler, instructions=instructions, content_type=content_type))


    @tm_app.task(queue="byzantium_q")
    def run_hydra(
            operator: dict,
            agent: dict,
            handler: str,
            use_input: str,
            instructions: str,
            content_type: str = "TEXT"
    ):

        use_operator = Operator(**operator)
        use_agent = SynodeAgent(**agent)

        hydra = cast(BaseHydra,BotFactory.summon(bot_name=use_operator.operator_path))
        hydra.timeout = use_agent.timeout

        return asyncio.run(Synode.run_hydra(operator=hydra, use_input=use_input, handler=handler, instructions=instructions,
                                          content_type=content_type,**use_agent.kwargs))


    @tm_app.task(queue="byzantium_q")
    def run_synode(
            operator: dict,
            agent: dict,
            handler: str,
            use_input: str,
            instructions: str,
    ):

        use_operator = Operator(**operator)
        use_agent = SynodeAgent(**agent)

        SynodeFactory.load_config(use_operator.operator_path, alias=use_operator.alias)
        synode = SynodeFactory.summon(use_operator.alias)

        return asyncio.run(Synode.run_synode(operator=synode, use_input=use_input, handler=handler, instructions=instructions))

    @tm_app.task(queue="byzantium_q")
    def run_basic(
            operator: dict,
            agent: dict,
            handler: str,
            use_input: str,
            instructions: str,
            blackboard: dict = None
    ):
        use_operator = Operator(**operator)
        use_agent = SynodeAgent(**agent)


        use_blackboard = None
        if blackboard:
            blackboard = BlackboardHandler(**blackboard)

            use_blackboard = SynodeHelpers.get_class(blackboard.handler)

            use_blackboard.from_dump(data=blackboard.data)

        klass = SynodeHelpers.get_class(use_operator.operator_path)

        result = None
        try:
            _kwargs = use_operator.kwargs.get("constructor", {})
            signature = inspect.signature(klass.__init__)

            # Extract parameter names, excluding 'self'
            param_names = [param.name for param in signature.parameters.values() if param.name != 'self']

            if use_blackboard and "blackboard" in param_names:
                operator = klass(blackboard=use_blackboard, **_kwargs)
            else:
                operator = klass(**_kwargs)

            result = asyncio.run(Synode.run_basic(
                operator=operator,
                handler=handler,
                use_input=use_input,
                instructions=instructions)
            )
        except Exception as e:
            Helpers.errPrint(e, "FAILED RUN BASIC IN byzantium tasks", 171)

        return result
