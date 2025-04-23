from kimera.helpers.Helpers import Helpers

from app.ext.byzantium.modules.Synode import Synode


class Krok(Synode):
    def start(self,*args, **kwargs):

        return "DO IT LIKE YOU MEAN IT NOW"

    async def before_any(self,*args, **kwargs):

        return "work on!"

    def finish(self, *args, **kwargs):

        return "DO IT LIKE YOU MEAN IT"
