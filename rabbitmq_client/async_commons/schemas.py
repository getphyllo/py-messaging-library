from pydantic import ConfigDict, BaseModel

from rabbitmq_client.async_commons.async_base_handler import AsyncBaseHandler


class ListenQueueConfig(BaseModel):
    name: str
    handler: AsyncBaseHandler
    model_config = ConfigDict(arbitrary_types_allowed=True)
