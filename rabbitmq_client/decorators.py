import functools
import logging
import pika.exceptions
import time


def retry_on_exception(max_retries=3, delay=1, backoff_factor=2,
                       retriable_exceptions=(pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError)):
    """
    A decorator to retry a function if it raises specific exceptions.

    :param max_retries: Maximum number of retries.
    :param delay: Initial delay between retries in seconds.
    :param backoff_factor: Factor by which the delay is multiplied on each retry.
    :param retriable_exceptions: Exceptions that trigger a retry.
    """

    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper_retry(*args, **kwargs):
            retries = 0
            current_delay = delay
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except retriable_exceptions as e:
                    logging.error(
                        f"Got retriable exception {e} on attempt {retries + 1}. Retrying after {current_delay} seconds...")
                    time.sleep(current_delay)
                    current_delay *= backoff_factor
                    retries += 1
                except Exception as e:
                    logging.error(f"Got non-retriable exception: {e}. No more retries.")
                    raise
            logging.error(f"Max retries reached ({max_retries}). Unable to complete the operation.")
            raise RuntimeError(f"Operation failed after {max_retries} retries.")

        return wrapper_retry

    return decorator_retry
