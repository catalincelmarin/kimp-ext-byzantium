from typing import Optional

from kimllm.gpt.BaseGPT import BaseGPT

from ..blackboard.Blackboard import Blackboard



class SynodeGPT(BaseGPT):
    def __init__(self, bot_name=None, *args, **kwargs):
        super().__init__(bot_name, *args, **kwargs)
        self._blackboard: Optional[Blackboard] = None

    @property
    def blackboard(self):
        return self._blackboard

    @blackboard.setter
    def blackboard(self, blackboard):
        self._blackboard = blackboard
