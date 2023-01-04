import functools
import json
import logging
import threading
from threading import Thread

from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.spec import Basic, BasicProperties

from rabbitmq_client.connection import get_connection
from rabbitmq_client.queue_config import ListenQueueConfig


class QueueListener(Thread):
    """
    This Listener takes care of processing incoming message in a MQ queue,
    calls the specific handler defined and acknowledges the message on successful processing.
    By default, auto_ack is kept as False, which avoid message loss in case of processing failures.
    auto_ack Refers to auto acknowledge of processed messages from MQ where
    if True MQ doesn't wait for any acknowledgement and message is removed once consumed.
    """
    def __init__(self, thread_id, queue_config: ListenQueueConfig):
        Thread.__init__(self, name=queue_config.name)
        self.thread_id = thread_id
        self.queue_config = queue_config
        self.connection = get_connection(queue_config.broker_config)
        logging.debug(f"Starting up thread {self.thread_id} and long-polling inbound queue {self.queue_config.name}")

    def run(self):
        """Start event of the listener thread."""
        logging.debug(f"Long polling queue {self.queue_config.name}")
        channel = self.connection.channel()

        def ack_message(ch: BlockingChannel, delivery_tag: int):
            """
            Note that `ch` must be the same pika channel instance via which
            the message being ACKed was retrieved (AMQP protocol constraint).
            """
            if ch.is_open:
                logging.debug(f"acknowledging message with delivery_tag {delivery_tag}")
                ch.basic_ack(delivery_tag)
            else:
                # Channel is already closed, so we can't ACK this message;
                # log and/or do something that makes sense for your app in this case.
                logging.debug("channel is already closed")
                pass

        def do_work(conn: BlockingConnection,
                    ch: BlockingChannel,
                    method: Basic.Deliver,
                    properties: BasicProperties,
                    delivery_tag: int,
                    body: bytes,
                    ):
            thread_id = threading.get_ident()
            logging.debug(f"Thread id: {thread_id} Delivery tag: {delivery_tag} Message body: {body}")
            self.queue_config.handler.handle_message(
                method=method,
                properties=properties,
                message=json.loads(body.decode("utf8"))
            )
            cb = functools.partial(ack_message, ch, delivery_tag)
            conn.add_callback_threadsafe(cb)

        def on_message(ch: BlockingChannel,
                       method: Basic.Deliver,
                       properties: BasicProperties,
                       body: bytes,
                       args: tuple,
                       ):
            logging.debug(f"Message received with body: {body}")
            (conn, thrds) = args
            delivery_tag = method.delivery_tag
            t = threading.Thread(target=do_work, args=(conn, ch, method, properties, delivery_tag, body))
            t.start()
            thrds.append(t)

        channel.basic_qos(prefetch_count=1)
        threads = []
        on_message_callback = functools.partial(on_message, args=(self.connection, threads))
        channel.basic_consume(queue=self.queue_config.name, on_message_callback=on_message_callback, auto_ack=False)

        logging.info(f"Waiting for data for {self.queue_config.name}")
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()

        # Wait for all to complete
        for thread in threads:
            thread.join()
        logging.debug("closing connection")
        self.connection.close()

    def stop(self):
        """Stop event of the listener thread."""
        logging.debug(f" [*] Thread  {self.thread_id} stopped")
