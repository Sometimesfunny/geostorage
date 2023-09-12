from typing import Any
import pika
from utils import from_json, to_json


class MQManager:
    all_queues: set[str] = set()

    def __init__(self, ip: str = "localhost") -> None:
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(ip))
        self.channel = self.connection.channel()

    def add_queue(self, name: str):
        self.channel.queue_declare(name)
        self.all_queues.add(name)

    def get_message(self, queue_name: str):
        for raw_data in self.channel.consume(queue_name, auto_ack=True):
            data = from_json(raw_data[2].decode())
            # self.channel.cancel()
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
