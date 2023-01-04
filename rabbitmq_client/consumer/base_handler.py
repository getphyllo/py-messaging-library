from abc import ABCMeta

from pika import spec


class BaseHandler(object):
    __metaclass__ = ABCMeta

    def handle_message(self, method: spec.Basic.Deliver, properties: spec.BasicProperties, message: dict):
        """
        Override this method to with your implementation for handling a message.
        Few things to look out for:
            1. In case of successful processing or in case where handler eats an exception,
            the processing will be treated successful and listener will acknowledge the message delivery.
            2. Add any retry logic in case of failures here.
        @param method: spec.Basic.Deliver
        @param properties: Pika Message Properties
        @param message: Message that was received by the queue
        """
        pass
