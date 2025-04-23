from kimera.helpers.Helpers import Helpers

from app.ext.byzantium.modules.SynodeBlackboard import SynodeBlackboard


class Krik:
    def __init__(self,blackboard: SynodeBlackboard = None,value=None,jmek=None):
        blackboard.set("outcome",False)
        blackboard.set("more",{"rock":{"paper":{"scissors":True}},"fitch":[1,2,3]})
        Helpers.sysPrint("value",value)
        Helpers.sysPrint("jmekk",jmek)

    async def work_me(self,use_input=None,*args, **kwargs):
        Helpers.sysPrint("WORKME",use_input)
        Helpers.sysPrint("KWARGS",kwargs)
        return "working::" + use_input