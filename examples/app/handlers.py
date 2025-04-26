from otteroad.consumer import BaseMessageHandler
from otteroad.models import TerritoryCreated  # please, use only models from the models/ directory


class TerritoryCreatedHandler(BaseMessageHandler[TerritoryCreated]):
    async def handle(self, event, ctx):
        print(f"Territory created: {event.territory_id}")

    async def on_startup(self): ...

    async def on_shutdown(self): ...
