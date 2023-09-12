from typing import Any
import pika
from utils import from_json, to_json
from pika.exceptions import AMQPConnectionError


class MQManager:
    all_queues: set[str] = set()

    def __init__(self, ip: str = "localhost") -> None:
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(ip))
        except AMQPConnectionError as e:
            print('RabbitMQ connection error! Please make sure RabbitMQ is running.')
            raise e
        self.channel = self.connection.channel()

    def add_queue(self, name: str):
        self.channel.queue_declare(name)
        self.all_queues.add(name)

    def get_message(self, queue_name: str):
        for raw_data in self.channel.consume(queue_name, auto_ack=True):
            data = from_json(raw_data[2].decode())
            break
        return data

    def send_message(self, queue_name: str, data: Any):
        self.channel.basic_publish("", queue_name, to_json(data))

    @classmethod
    def delete_all_queues(cls, ip: str = "localhost"):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(ip))
        except:
            exit(1)
        channel = connection.channel()
        for queue_name in cls.all_queues:
            channel.queue_delete(queue_name)
