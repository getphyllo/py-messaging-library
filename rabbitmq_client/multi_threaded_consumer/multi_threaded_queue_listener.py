import asyncio
import functools
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from pika import SelectConnection
from pika.channel import Channel as PikaChannel
from pika.spec import Basic as PikaBasic, BasicProperties as PikaBasicProperties
from rabbitmq_client.async_connection.connection import get_async_connection
from rabbitmq_client.broker_config import BrokerConfig
from rabbitmq_client.config import settings
from rabbitmq_client.consumer.health_utils import set_consumer_status
from rabbitmq_client.async_commons.schemas import ListenQueueConfig


class MultiThreadedQueueListener(object):
    """
    This is a multi-threaded multi-queue listener. This listener implements threading
    to enable us to handle messages concurrently.
    """
    def __init__(self, broker_config: BrokerConfig, listen_queue_config: ListenQueueConfig, num_threads: int = 5):
        logging.debug('Creating connection object and registering callbacks')
        self.connection = get_async_connection(broker_config=broker_config,
                                               on_connection_open=self.on_connection_open,
                                               on_connection_closed=self.on_connection_closed)
        self.listen_queue_config = listen_queue_config
        self.num_threads = num_threads
        self.executor = ThreadPoolExecutor(max_workers=num_threads)
        self.channels = []

    def on_connection_open(self, connection: SelectConnection):
        for _ in range(self.num_threads):
            connection.channel(on_open_callback=self.on_channel_open)

    def on_connection_closed(self, connection, reason):
        logging.exception('Connection closed: %s', reason)
        raise Exception('Connection closed: %s', reason)

    def on_channel_open(self, channel: PikaChannel):
        self.channels.append(channel)
        channel.add_on_close_callback(self.on_channel_closed)

        if len(self.channels) == self.num_threads:
            self.start_consuming()

    def start_consuming(self):
        for channel in self.channels:
            self.setup_channel(channel)

    def setup_channel(self, channel: PikaChannel):
        def on_message(ch: PikaChannel, method: PikaBasic.Deliver,
                       properties: PikaBasicProperties, body: bytes, args):
            message = body.decode('utf8')
            handler = args[0]
            # Run the message handling in a separate thread
            self.executor.submit(self._process_message, handler, method, properties, message, ch)

        channel.basic_qos(prefetch_count=settings.PREFETCH_COUNT)
        queue_name: str = self.listen_queue_config.name
        on_message_callback = functools.partial(on_message, args=(self.listen_queue_config.handler,))
        channel.basic_consume(queue=queue_name, on_message_callback=on_message_callback, auto_ack=False)

    def _process_message(self, handler, method, properties, message, channel):
        current_thread = threading.current_thread().name
        logging.info(f"Processing message in thread: {current_thread}")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(
                handler.async_handle_message(
                    method=method, properties=properties, message=message
                )
            )
            if channel.is_open:
                logging.debug('Channel is open, acknowledging the message')
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.warning('Channel closed, unable to ack message')
        except Exception as e:
            logging.error(f"Error processing message: {e}")
        finally:
            loop.close()

    def on_channel_closed(self, ch, reason):
        logging.exception('Channel %i was closed: %s', ch, reason)
        raise Exception('Channel %i was closed: %s', ch, reason)

    def listen(self):
        try:
            logging.info('Starting IO Loop to listen for messages')
            set_consumer_status(is_healthy=True)
            self.connection.ioloop.start()
        except KeyboardInterrupt:
            logging.info('Keyboard Interrupt Closing')
            self.connection.close()
        except Exception as ex:
            set_consumer_status(is_healthy=False)
            self.connection.close()
            raise ex
