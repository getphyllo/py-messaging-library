import functools
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from threading import Thread

from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.spec import Basic, BasicProperties

from rabbitmq_client.config import settings
from rabbitmq_client.connection import get_connection
from rabbitmq_client.consumer.health_utils import set_consumer_status
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

        def republish_message(ch, exchange, routing_key, properties, body, delivery_tag):
            ch.basic_publish(exchange=exchange, routing_key=routing_key, properties=properties, body=body)
            ack_message(ch, delivery_tag)

        def handle_error(ch, method_frame, _header_frame, delivery_tag, body):
            if self.queue_config.error_queue:
                logging.info(
                    f"Publishing message to error queue with exchange: {self.queue_config.error_queue.exchange}"
                    f" and routing key: {self.queue_config.error_queue.routing_key}")
                ch.basic_publish(exchange=self.queue_config.error_queue.exchange,
                                 routing_key=self.queue_config.error_queue.routing_key, properties=_header_frame,
                                 body=body)
                ack_message(ch, delivery_tag)
            elif self.queue_config.requeue_on_failure:
                # TODO this to be monitored for the frequency and see if can create infinite loop issues.
                logging.info(f"Republishing message back to queue {self.queue_config.name}")
                republish_message(ch, method_frame.exchange, method_frame.routing_key, _header_frame, body,
                                  delivery_tag)
            else:
                logging.warning(f"Acknowledging message {body} on failure")
                ack_message(ch, delivery_tag)

        def do_work(conn: BlockingConnection,
                    ch: BlockingChannel,
                    method: Basic.Deliver,
                    properties: BasicProperties,
                    delivery_tag: int,
                    body: bytes,
                    ):
            thread_id = threading.get_ident()
            logging.debug(f"Thread id: {thread_id} Delivery tag: {delivery_tag} Message body: {body}")
            try:
                self.queue_config.handler.handle_message(
                    method=method,
                    properties=properties,
                    message=json.loads(body.decode("utf8"))
                )
                cb = functools.partial(ack_message, ch, delivery_tag)
            except Exception as ex:
                # This is needed when auto_ack is False.
                # It stops the consumption of further messages until an ack is received.
                logging.error(f"Message handling failed for message {body} with exception: {ex}.")
                cb = functools.partial(handle_error, ch, method, properties, delivery_tag, body)
            conn.add_callback_threadsafe(cb)

        def on_message(ch: BlockingChannel,
                       method: Basic.Deliver,
                       properties: BasicProperties,
                       body: bytes,
                       args: tuple,
                       ):
            logging.debug(f"Message received with body: {body}")
            (conn, thread_pool_executor) = args
            delivery_tag = method.delivery_tag
            thread_pool_executor.submit(do_work, conn, ch, method, properties, delivery_tag, body)

        channel.basic_qos(prefetch_count=settings.PREFETCH_COUNT)

        thread_pool_executor_kwargs = {}
        if settings.THREAD_POOL_WORKER_COUNT is not None:
            thread_pool_executor_kwargs['max_workers'] = settings.THREAD_POOL_WORKER_COUNT
        thread_pool_executor = ThreadPoolExecutor(**thread_pool_executor_kwargs)

        on_message_callback = functools.partial(on_message, args=(self.connection, thread_pool_executor))
        channel.basic_consume(queue=self.queue_config.name, on_message_callback=on_message_callback, auto_ack=False)

        logging.info(f"Waiting for data for {self.queue_config.name}")
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
        except Exception as ex:
            logging.error(
                f"Failure received while consuming {self.queue_config.name} queue: {type(ex)}, {ex}, exiting.")
            set_consumer_status(is_healthy=False)

        # Wait for all to complete
        thread_pool_executor.shutdown(wait=True)
        logging.debug("closing connection")
        self.connection.close()

    def stop(self):
        """Stop event of the listener thread."""
        logging.debug(f" [*] Thread  {self.thread_id} stopped")
