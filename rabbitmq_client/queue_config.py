from typing import Optional

from pydantic import ConfigDict, BaseModel

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
    requeue_on_failure: Optional[bool] = True
    error_queue: Optional[PublishQueueConfig] = None
    model_config = ConfigDict(arbitrary_types_allowed=True)
