# Python Messaging Library
Library simplifying messaging queue integrations in Python. 
It supports RabbitMQ at this point which is a wrapper on top of [Pika](https://github.com/pika/pika).
The goal is to extend to more Messaging Platforms.

#### Installation
If you are using `pip` then  
`pip install git+https://github.com/getphyllo/py-messaging-library.git@<version_tag>`  
Ex: `pip install git+https://github.com/getphyllo/messaging-library.git@v1.0.0`  
More information [here](https://pip.pypa.io/en/stable/topics/vcs-support/)

If you are using `pipenv`  
`pipenv install git+ssh://git@github.com/getphyllo/py-messaging-library@{version_tag}#egg=py_messaging_library`  
`pipenv install git+https://github.com/getphyllo/py-messaging-library.git@{version_tag}#egg=py_messaging_library`  
Ex:
`pipenv install git+ssh://git@github.com/getphyllo/py-messaging-library@v1.0.0#egg=py_messaging_library`  
`pipenv install git+https://github.com/getphyllo/py-messaging-library.git@v1.0.0#egg=py_messaging_library`  
For more information search for `Installing from git:` [here](https://github.com/pypa/pipenv#-usage)

#### Usage
##### Publish and Consume messages - Below is the Sample code for integrating with this library. 

###### config.py
```python
from rabbitmq_client.broker_config import BrokerConfig

broker_config = BrokerConfig(username="user-test",
                             password="user-test",
                             host="localhost",
                             port=5671,
                             vhost="/",
                             ssl_enabled=False)

```

###### publish.py
```python
from datetime import datetime
from uuid import UUID

from rabbitmq_client.publisher import publisher
from rabbitmq_client.queue_config import PublishQueueConfig

from config import broker_config


@publisher.register_serializer
def custom_serializer(obj):
    if isinstance(obj, datetime):
        return obj.__str__()
    if isinstance(obj, UUID):
        return str(obj)


def publish_message(message: dict, routing_key: str, exchange_name: str):
    publish_queue_config: PublishQueueConfig = PublishQueueConfig(routing_key=routing_key,
                                                                  exchange=exchange_name,
                                                                  broker_config=broker_config)
    publisher.publish(queue_config=publish_queue_config, payload=message)
```


###### handler.py
```python
import logging

from pika import spec
from rabbitmq_client.consumer.base_handler import BaseHandler


class DemoHandler(BaseHandler):
    def handle_message(self, method: spec.Basic.Deliver, properties: spec.BasicProperties, message: dict):
        print(f'Processing message {message}')
        # do some work
        logging.info('Message processed successfully')

```

###### consumer.py
```python
from rabbitmq_client.consumer.multi_queue_listener import MultiQueueListener
from rabbitmq_client.queue_config import ListenQueueConfig

from broker_config import MQ_BROKER
from handler import DemoHandler

queue1 = ListenQueueConfig(name="queue_name",
                          handler=DemoHandler(),
                          broker_config=MQ_BROKER)

mq_listener = MultiQueueListener([queue1])
mq_listener.listen()
```

###### demo.py
```python
import logging

from rabbitmq_client.consumer.multi_queue_listener import MultiQueueListener
from rabbitmq_client.queue_config import ListenQueueConfig

from config import broker_config
from handler import DemoHandler
from publish import publish_message


if __name__ == '__main__':
    # Sending message to exchange with a routing key
    message = {'key': 'value'}
    logging.info('Sending message')
    publish_message(message=message, exchange_name='demo-exchange', routing_key='.queue-1.queue-2.')
    logging.info('Message sent')

    # Processing messages from queue
    queue1 = ListenQueueConfig(name='demo-queue-1',
                               handler=DemoHandler(),
                               broker_config=broker_config)
    queue2 = ListenQueueConfig(name='demo-queue-2',
                               handler=DemoHandler(),
                               broker_config=broker_config)

    multi_queue_listener = MultiQueueListener([queue1, queue2])
    multi_queue_listener.listen()
```

When you run `demo.py` the script pushes message to Rabbit MQ, and also starts a consumer on those queues
