import logging
from abc import ABCMeta
from typing import List

from rabbitmq_client.consumer.health_utils import set_consumer_status
from rabbitmq_client.consumer.poller import QueueListener
from rabbitmq_client.queue_config import ListenQueueConfig


class MultiQueueListener(object):
    """
    Main class: instantiates the listeners and runs the main event loop.
    This abstract class must implement handle_message method to be instantiated.
    """

    __metaclass__ = ABCMeta

    def __init__(self, queues_configuration: List[ListenQueueConfig]):
        self._queues_configuration = queues_configuration

    def _start_listeners(self):
        threads = []
        for index, queue_config in enumerate(self._queues_configuration):
            logging.debug(f"Launching listener for {queue_config.name} in thread {index}")
            listener_thread = QueueListener(thread_id=index, queue_config=queue_config)
            listener_thread.start()
            logging.debug("started listener thread")
            threads.append(listener_thread)

        for thread in threads:
            logging.debug(f"joining thread id {thread.thread_id}")
            thread.join()

    def listen(self):
        """Start listeners threads."""
        logging.debug('Registering listeners on all queues')
        set_consumer_status(is_healthy=True)
        self._start_listeners()
