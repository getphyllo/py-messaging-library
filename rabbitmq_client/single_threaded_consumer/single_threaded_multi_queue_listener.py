import asyncio
import functools
import json
import logging

from pika import SelectConnection
from pika.channel import Channel as PikaChannel
from pika.spec import Basic as PikaBasic, BasicProperties as PikaBasicProperties

from rabbitmq_client.async_connection.connection import get_async_connection
from rabbitmq_client.broker_config import BrokerConfig
from rabbitmq_client.config import settings
from rabbitmq_client.consumer.health_utils import set_consumer_status
from rabbitmq_client.single_threaded_consumer.schemas import ListenQueueConfig


class SingleThreadedMultiQueueListener(object):
    """
    This is a single threaded multi queue listener, as opposed the other multi queue listener in the library.
    this listener does not implement threads, which enables us to trigger async methods while handling the messages
    """
    def __init__(self, broker_config: BrokerConfig, listen_queue_configs: list[ListenQueueConfig]):
        logging.debug('Creating connection object and registering callbacks')
        self.connection = get_async_connection(broker_config=broker_config,
                                               on_connection_open=self.on_connection_open,
                                               on_connection_closed=self.on_connection_closed)
        self.listen_queue_configs = listen_queue_configs

    def on_connection_open(self, connection: SelectConnection):
        connection.channel(on_open_callback=self.on_channel_open)

    def on_connection_closed(self, connection, reason):
        logging.exception('Connection closed: %s', reason)
        raise Exception('Connection closed: %s', reason)

    def on_channel_open(self, channel: PikaChannel):
        channel.add_on_close_callback(self.on_channel_closed)

        def on_message(ch: PikaChannel, method: PikaBasic.Deliver,
                       properties: PikaBasicProperties, body: bytes, args):
            message = json.loads(body.decode('utf8'))
            loop = asyncio.get_event_loop()
            (handler,) = args
            loop.run_until_complete(
                handler.async_handle_message(
                    method=method, properties=properties, message=message
                )
            )
            if ch.is_open:
                logging.debug('Channel is open, acknowledging the message')
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.warning('Channel closed unable to ack message')

        channel.basic_qos(prefetch_count=settings.PREFETCH_COUNT)
        for listen_queue_config in self.listen_queue_configs:
            queue_name: str = listen_queue_config.name
            on_message_callback = functools.partial(on_message, args=(listen_queue_config.handler,))
            channel.basic_consume(queue=queue_name, on_message_callback=on_message_callback, auto_ack=False)

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
