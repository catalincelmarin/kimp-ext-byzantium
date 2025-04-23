from kimera.helpers.Helpers import Helpers

from app.ext.byzantium.modules.Synode import Synode


class Krak(Synode):

    async def before_any(self,*args, **kwargs):
        chunk = """
        The climate crisis is one of the most urgent challenges humanity faces today. Rising temperatures, extreme weather patterns,
         and the loss of biodiversity are just a few of the consequences. 
        Immediate action is needed to mitigate the impacts and create a sustainable future for generations to come.
        """
        return chunk

    async def before_summarize(self,use_input, *args, **kwargs):
        data = "|".join(use_input)
        return "compare and conclude from variants: " + data + "\n --------- \n also return the updated and optimized best variant"

    def finish(self, *args, **kwargs):

        return "DO IT LIKE YOU MEAN IT"

    def done_process(self,result):
        Helpers.sysPrint("RESULT",result)