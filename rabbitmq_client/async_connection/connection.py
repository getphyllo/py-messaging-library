import ssl
from typing import Callable

import pika

from rabbitmq_client.broker_config import BrokerConfig


def get_async_connection(broker_config: BrokerConfig,
                         on_connection_open: Callable,
                         on_connection_closed: Callable
                         ) -> pika.SelectConnection:
    credentials = pika.PlainCredentials(username=broker_config.username,
                                        password=broker_config.password.get_secret_value(),
                                        erase_on_connect=True)
    connection_parameters = {
        'host': broker_config.host,
        'port': broker_config.port,
        'virtual_host': broker_config.vhost,
        'credentials': credentials,
        'heartbeat': broker_config.heartbeat,
    }
    if broker_config.ssl_enabled:
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        connection_parameters['ssl_options'] = pika.SSLOptions(context)

    parameters = pika.ConnectionParameters(**connection_parameters)

    return pika.SelectConnection(parameters=parameters,
                                 on_open_callback=on_connection_open,
                                 on_close_callback=on_connection_closed)
