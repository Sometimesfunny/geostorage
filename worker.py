from constants import COMMAND_META, COMMAND_QUIT, COMMAND_ROAD_BY_ID
from mq_manager import MQManager


class Worker:
    def __init__(self, worker_id: int):
        self.worker_id: int = worker_id
        self.data: dict[int, dict[int, list[float]]] = None
        self.mq_manager = MQManager()
        self.mq_manager.add_queue(f"worker_{self.worker_id}")
        self.mq_manager.add_queue("manager")

    def proceed_requests(self):
        while True:
            data = self.mq_manager.get_message(f"worker_{self.worker_id}")
            if data.get("command") == COMMAND_QUIT:
                return
            elif data.get("command") == COMMAND_ROAD_BY_ID:
                road_id = data.get("data", -1)
                self.mq_manager.send_message(
                    f"manager",
                    {
                        "command": COMMAND_ROAD_BY_ID,
                        "block_number": self.worker_id,
                        "data": self.data.get(str(road_id), {}),
                    },
                )
            elif data.get("command") == COMMAND_META:
                self.data = data.get("data", {})


if __name__ == "__main__":
    worker_id = int(input("Input worker id: "))
    worker = Worker(worker_id)
    worker.proceed_requests()
