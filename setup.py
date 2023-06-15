from setuptools import setup

setup(
    name='py_messaging_library',
    version='1.0.0',
    packages=['rabbitmq_client', 'rabbitmq_client/consumer'],
    url='',
    license='',
    author='',
    author_email='',
    description='This Library simplifies messaging queue integrations in Python. '
                'It supports RabbitMQ at this point, having a wrapper over '
                'pika which is the officially recommended client for Rabbit MQ.',
    install_requires=[
        'pika==1.3.1',
        'pydantic==1.10.5',
        'starlette==0.25.0'
    ]
)
