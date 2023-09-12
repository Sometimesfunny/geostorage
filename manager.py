from collections import defaultdict
from constants import COMMAND_ALL_ROADS, COMMAND_META, COMMAND_QUIT, COMMAND_ROAD_BY_ID
from mq_manager import MQManager


class Manager:
    def __init__(self) -> None:
        self.meta: list[dict] = None
        self.blocks_number = 0
        self.mq_manager = MQManager()
        self.mq_manager.add_queue("client")
        self.mq_manager.add_queue("manager")

    def proceed_requests(self):
        while True:
            data = self.mq_manager.get_message("manager")
            if data.get("command") == COMMAND_ROAD_BY_ID:
                data = self.get_road(data["data"])
                self.mq_manager.send_message("client", {"command": COMMAND_ROAD_BY_ID, "data": data if data else []})
            elif data.get("command") == COMMAND_ALL_ROADS:
                self.mq_manager.send_message("client", {"command": COMMAND_ALL_ROADS, "data": self.get_road_ids()})
            elif data.get("command") == COMMAND_QUIT:
                self.exit()
                return
            elif data.get("command") == COMMAND_META:
                self.meta = data.get("meta", [])
                self.blocks_number = data.get("blocks_number", 0)
                for i in range(self.blocks_number):
                    self.mq_manager.add_queue(f"worker_{i}")

    def exit(self):
        for i in range(self.blocks_number):
            self.mq_manager.send_message(
                f"worker_{i}", {"command": COMMAND_QUIT, "data": "quit"}
            )

    def get_road_ids(self):
        return [x["id"] for x in self.meta]

    def get_road(self, road_id: int):
        road_id = int(road_id)
        road_meta = {}
        for item in self.meta:
            if int(item.get("id")) == road_id:
                road_meta = item
                break
        else:
            return
        parts_info = road_meta.get("parts_info")
        block_numbers = set()
        for part in parts_info:
            block_numbers.add(part[0])
        block_data = {}
        for block_number in block_numbers:
            self.mq_manager.send_message(f"worker_{block_number}", {"command": COMMAND_ROAD_BY_ID, "data": road_id})
        for block_number in block_numbers:
            data = self.mq_manager.get_message(f"manager")
            block_data[data.get("block_number")] = data.get("data")
        data_pointers = defaultdict(int)
        output_data: list[list] = []
        for part in parts_info:
            output_data.extend(
                block_data[part[0]][
                    data_pointers[part[0]] : data_pointers[part[0]] + part[1]
                ]
            )
            data_pointers[part[0]] += part[1]
        return output_data
