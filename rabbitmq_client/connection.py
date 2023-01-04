import ssl

import pika

from rabbitmq_client.broker_config import BrokerConfig


def get_connection(broker_config: BrokerConfig):
    credentials = pika.PlainCredentials(username=broker_config.username,
                                        password=broker_config.password.get_secret_value(),
                                        erase_on_connect=True)
    connection_parameters = {
        'host': broker_config.host,
        'port': broker_config.port,
        'virtual_host': broker_config.vhost,
        'credentials': credentials,
        'heartbeat': 10,
    }
    if broker_config.ssl_enabled:
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        connection_parameters['ssl_options'] = pika.SSLOptions(context)

    parameters = pika.ConnectionParameters(**connection_parameters)

    return pika.BlockingConnection(parameters)
