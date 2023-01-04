from pydantic import BaseModel

from rabbitmq_client.broker_config import BrokerConfig
from rabbitmq_client.consumer.base_handler import BaseHandler


class BaseQueueConfig(BaseModel):
    broker_config: BrokerConfig


class PublishQueueConfig(BaseQueueConfig):
    exchange: str
    routing_key: str


class ListenQueueConfig(BaseQueueConfig):
    name: str
    handler: BaseHandler

    class Config:
        arbitrary_types_allowed = True
