from pydantic import ConfigDict, BaseModel

from rabbitmq_client.single_threaded_consumer.async_base_handler import AsyncBaseHandler


class ListenQueueConfig(BaseModel):
    name: str
    handler: AsyncBaseHandler
    model_config = ConfigDict(arbitrary_types_allowed=True)
