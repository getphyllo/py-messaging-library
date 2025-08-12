import asyncio
import functools
import json
import logging
import threading

from pika import SelectConnection
from pika.channel import Channel as PikaChannel
from pika.spec import Basic as PikaBasic, BasicProperties as PikaBasicProperties

from rabbitmq_client.async_connection.connection import get_async_connection
from rabbitmq_client.broker_config import BrokerConfig
from rabbitmq_client.config import settings
from rabbitmq_client.consumer.health_utils import set_consumer_status
from rabbitmq_client.single_threaded_consumer.schemas import ListenQueueConfig


class SingleThreadedMultiQueueListener:
    """
    This is a single-threaded, multi-queue RabbitMQ consumer that supports async message handlers.
    It avoids threads for message processing, enabling async API calls (like PDF generation).
    """
    def __init__(self, broker_config: BrokerConfig, listen_queue_configs: list[ListenQueueConfig]):
        logging.debug('Creating connection object and registering callbacks')
        self.connection = get_async_connection(
            broker_config=broker_config,
            on_connection_open=self.on_connection_open,
            on_connection_closed=self.on_connection_closed
        )
        self.listen_queue_configs = listen_queue_configs

    def _start_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def on_connection_open(self, connection: SelectConnection):
        connection.channel(on_open_callback=self.on_channel_open)

    def on_connection_closed(self, connection, reason):
        logging.exception('Connection closed: %s', reason)
        raise Exception(f'Connection closed: {reason}')

    def on_channel_open(self, channel: PikaChannel):
        channel.add_on_close_callback(self.on_channel_closed)

        # Set prefetch to 1 to process one message at a time
        channel.basic_qos(prefetch_count=settings.PREFETCH_COUNT)

        def on_message(ch: PikaChannel, method: PikaBasic.Deliver,
                       properties: PikaBasicProperties, body: bytes, args):
            (handler,) = args
            logging.debug("Received message: %s", body)
            message = json.loads(body.decode('utf8'))

            async def handle_and_ack():
                logging.debug("Starting async handler for delivery_tag=%s", method.delivery_tag)
                await handler.async_handle_message(
                    method=method, properties=properties, message=message
                )
                if ch.is_open:
                    logging.debug('Acknowledging message with delivery_tag=%s', method.delivery_tag)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    logging.warning('Channel closed before ack')

            # Submit task to background asyncio loop
            self.loop.call_soon_threadsafe(lambda: asyncio.ensure_future(handle_and_ack(), loop=self.loop))

        for listen_queue_config in self.listen_queue_configs:
            queue_name: str = listen_queue_config.name
            on_message_callback = functools.partial(on_message, args=(listen_queue_config.handler,))
            channel.basic_consume(queue=queue_name, on_message_callback=on_message_callback, auto_ack=False)
            logging.info("Consuming from queue: %s", queue_name)

    def on_channel_closed(self, ch, reason):
        logging.exception('Channel %i was closed: %s', ch, reason)
        raise Exception(f'Channel {ch} was closed: {reason}')

    def listen(self):
        try:
            logging.info('Starting background asyncio event loop for async message handlers')
            # Set up a
            self.loop = asyncio.new_event_loop()
            self.loop_thread = threading.Thread(target=self._start_loop, daemon=True)
            self.loop_thread.start()

            logging.info('Starting IO Loop to listen for messages')
            set_consumer_status(is_healthy=True)
            self.connection.ioloop.start()
        except KeyboardInterrupt:
            logging.info('Keyboard Interrupt Closing')
            self.stop()
        except Exception as ex:
            set_consumer_status(is_healthy=False)
            self.stop()
            raise ex

    def stop(self):
        logging.info("Stopping consumer...")

        # Stop pika I/O loop if running
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            if self.connection and self.connection.ioloop.is_running:
                self.connection.ioloop.stop()
        except Exception as e:
            logging.warning(f"Error stopping pika loop: {e}")

        # Stop asyncio loop
        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)

        # Wait for background loop thread to finish
        if hasattr(self, "loop_thread") and self.loop_thread.is_alive():
            self.loop_thread.join(timeout=5)

        logging.info("Consumer stopped.")

