from pydantic import BaseModel

from rabbitmq_client.single_threaded_consumer.async_base_handler import AsyncBaseHandler


class ListenQueueConfig(BaseModel):
    name: str
    handler: AsyncBaseHandler

    class Config:
        arbitrary_types_allowed = True
