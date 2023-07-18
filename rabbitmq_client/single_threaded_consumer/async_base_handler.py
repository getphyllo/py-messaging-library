from abc import ABCMeta, abstractmethod

from pika.spec import Basic as PikaBasic, BasicProperties as PikaBasicProperties


class AsyncBaseHandler(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    async def async_handle_message(self, method: PikaBasic.Deliver, properties: PikaBasicProperties, message: dict):
        pass
