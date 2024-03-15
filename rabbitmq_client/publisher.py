import json
import logging
from datetime import datetime, date
from uuid import UUID

import pika.exceptions

from rabbitmq_client.connection import get_connection
from rabbitmq_client.exceptions import PublisherException
from rabbitmq_client.queue_config import PublishQueueConfig
from typing import Callable, Optional, List


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
                headers: Optional[dict] = None, max_retries: int = 3, priority: Optional[int] = 0):
        if headers is None:
            headers = {}
        assert isinstance(queue_config, PublishQueueConfig), \
            f"Expected instance of PublishQueueConfig, passed {type(queue_config)}"
        assert self.serializer is not None, \
            "Define custom serializer with @publish.register_serializer decorator"
        assert isinstance(headers, dict), f"Expected instance of dict, passed {type(headers)}"
        assert isinstance(priority, int), \
            f"Expected instance of int, passed {type(priority)}"

        if payload_list is None:
            payload_list = [payload]

        for payload_item in payload_list:
            assert isinstance(payload_item, dict), \
                f"Expected instance of dict, passed {type(payload_item)}"

        retries = 0
        while retries < max_retries:
            try:
                self._publish_to_channel(queue_config, payload_list, headers, priority)
                return
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as retriable_exception:
                logging.error(f"Got retriable exception {retriable_exception} "
                              f"for queue {queue_config.routing_key} while creating channel")
            except Exception as non_retriable_exception:
                logging.error(f"Got Non-retriable exception: {non_retriable_exception} "
                              f"for queue {queue_config.routing_key} while creating channel")
                raise

            retries += 1
            if retries >= max_retries:
                logging.error(f"Max retries reached. Unable to make connection with queue {queue_config.routing_key}")
                raise

    def _publish_to_channel(self, queue_config: PublishQueueConfig, payloads: List[dict],
                            headers: Optional[dict] = None, priority: Optional[int] = 0):
        with get_connection(queue_config.broker_config) as connection:
            channel = connection.channel()
            for payload in payloads:
                stringified_payload = json.dumps(payload, default=self.serializer).encode('utf-8')
                # ToDo: Fix this. Below queue_declare raises conflict if it(durable) does not match with definitions.json config
                # channel.queue_declare(queue=queue_config.name)
                try:
                    channel.basic_publish(exchange=queue_config.exchange,
                                          routing_key=queue_config.routing_key,
                                          body=stringified_payload,
                                          properties=pika.BasicProperties(headers=headers, priority=priority))
                except Exception as e:
                    logging.error(f"Got exception {e} while publishing message to queue {queue_config.routing_key}")
                    raise PublisherException(f"Got exception {e} while publishing message to queue "
                                             f"{queue_config.routing_key} for payload {payload}")


publisher = Publisher()
