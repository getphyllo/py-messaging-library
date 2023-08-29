import json
from datetime import datetime, date
from uuid import UUID

import pika

from rabbitmq_client.connection import get_connection
from rabbitmq_client.queue_config import PublishQueueConfig
from typing import Callable, Optional


def default_serializer(obj):
    if isinstance(obj, datetime):
        return obj.__str__()
    if isinstance(obj, date):
        return obj.__str__()
    if isinstance(obj, UUID):
        return str(obj)


class DuplicateSerializerError(Exception):
    pass


class Publisher:
    serializer = None

    def register_serializer(self, func: Callable):
        """
        Decorator to register serializers, refer ReadMe.MD for example usage
        @param func: callable, the actual serializer method
        """
        if self.serializer is not None:
            raise DuplicateSerializerError(f"Serializer already assigned with name '{self.serializer.__name__}'")
        self.serializer = func

    def publish(self, queue_config: PublishQueueConfig, payload: dict, headers: Optional[dict] = None,
                priority: Optional[int] = 0):
        if headers is None:
            headers = {}
        assert isinstance(queue_config, PublishQueueConfig), \
            f"Expected instance of PublishQueueConfig, passed {type(queue_config)}"
        assert isinstance(payload, dict), \
            f"Expected instance of dict, passed {type(payload)}"
        assert isinstance(headers, dict), \
            f"Expected instance of dict, passed {type(headers)}"
        assert isinstance(priority, int), \
            f"Expected instance of int, passed {type(priority)}"

        serializer: Callable = self.serializer or default_serializer
        stringified_payload = json.dumps(payload, default=serializer).encode('utf-8')
        with get_connection(queue_config.broker_config) as connection:
            channel = connection.channel()
            channel.basic_publish(exchange=queue_config.exchange,
                                  routing_key=queue_config.routing_key,
                                  body=stringified_payload,
                                  properties=pika.BasicProperties(headers=headers, priority=priority))


publisher = Publisher()
