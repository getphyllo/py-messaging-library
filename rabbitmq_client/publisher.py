import json
import logging
from datetime import datetime, date
from typing import Callable, Optional, List
from uuid import UUID

import pika

from rabbitmq_client.connection import get_connection
from rabbitmq_client.queue_config import PublishQueueConfig


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

    def publish(self, queue_config: PublishQueueConfig,
                payload: Optional[dict] = None, payload_list: List[dict] = None,
                headers: Optional[dict] = None, priority: Optional[int] = 0,
                expiration: Optional[int] = None, delivery_mode: Optional[int] = 1):
        if headers is None:
            headers = {}

        # Check that either payload or payload_list is provided, but not both None
        if payload is None and (payload_list is None or len(payload_list) == 0):
            raise ValueError("Either 'payload' or 'payload_list' must be provided.")

        if payload_list is None or len(payload_list) == 0:
            payload_list = [payload]

        self._validate_publish_arguments(queue_config, payload_list, headers, priority, expiration, delivery_mode)
        self._publish_to_channel_atomically(queue_config, payload_list, headers, priority, expiration, delivery_mode)

    @staticmethod
    def _validate_publish_arguments(queue_config: PublishQueueConfig, payload_list: Optional[List[dict]],
                                    headers: dict, priority: int, expiration: Optional[int],
                                    delivery_mode: Optional[int]):
        assert isinstance(queue_config, PublishQueueConfig), "Expected instance of PublishQueueConfig"
        assert isinstance(headers, dict), "Expected instance of dict for headers"
        assert isinstance(priority, int), "Expected instance of int for priority"

        if expiration is not None:
            assert isinstance(expiration, int), "Expected instance of int for expiration (milliseconds)"
            assert expiration > 0, "Expiration must be a positive integer (milliseconds)"

        if delivery_mode is not None:
            assert isinstance(delivery_mode, int), "Expected instance of int for delivery_mode"
            assert delivery_mode in [1, 2], "Delivery mode must be 1 (transient) or 2 (persistent)"

        for payload_item in payload_list:
            assert isinstance(payload_item, dict), "All items in 'payload_list' must be dictionaries."

    def _publish_to_channel_atomically(self, queue_config: PublishQueueConfig, payloads: List[dict],
                                       headers: Optional[dict] = None, priority: Optional[int] = 0,
                                       expiration: Optional[int] = None, delivery_mode: Optional[int] = 1):
        serializer: Callable = self.serializer or default_serializer
        with get_connection(queue_config.broker_config) as connection:
            channel = connection.channel()
            channel.tx_select()
            for payload in payloads:
                stringified_payload = json.dumps(payload, default=serializer).encode('utf-8')
                # ToDo: Fix this. Below queue_declare raises conflict if it(durable) does not match with definitions.json config
                # channel.queue_declare(queue=queue_config.name)
                try:
                    properties_kwargs = {
                        'headers': headers,
                        'priority': priority
                    }

                    if delivery_mode is not None:
                        properties_kwargs['delivery_mode'] = delivery_mode

                    if expiration is not None:
                        properties_kwargs['expiration'] = str(expiration)

                    channel.basic_publish(exchange=queue_config.exchange,
                                          routing_key=queue_config.routing_key,
                                          body=stringified_payload,
                                          properties=pika.BasicProperties(**properties_kwargs))
                except Exception as e:
                    logging.error(f"Got exception {e} while publishing message to queue {queue_config.routing_key}")
                    raise
            channel.tx_commit()


publisher = Publisher()
