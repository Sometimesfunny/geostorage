from dataclasses import dataclass
import json
from collections import defaultdict
from constants import COMMAND_META
from utils import calc_closest_factors
from mq_manager import MQManager


@dataclass
class MapRectangle:
    long1: float = 180
    lat1: float = 90
    long2: float = -180
    lat2: float = -90

    def update_rectangle(self, value: "MapRectangle") -> None:
        self.long1 = min(value.long1, self.long1)
        self.lat1 = min(value.lat1, self.lat1)
        self.long2 = max(value.long2, self.long2)
        self.lat2 = max(value.lat2, self.lat2)

    def __contains__(self, item: list[float]) -> bool:
        return (self.long1 <= item[0] <= self.long2) and (
            self.lat1 <= item[1] <= self.lat2
        )


class Map:
    def __init__(
        self, map_rectangle: MapRectangle, rows_amount: int, columns_amount: int
    ) -> None:
        self.map_rectangle = map_rectangle
        self.rows_amount = rows_amount
        self.columns_amount = columns_amount
        self.blocks: list[MapRectangle] = []
        self.create_blocks()

    def create_blocks(self):
        long_diff: float = (
            self.map_rectangle.long2 - self.map_rectangle.long1
        ) / self.columns_amount
        lat_diff: float = (
            self.map_rectangle.lat2 - self.map_rectangle.lat1
        ) / self.rows_amount
        for row_n in range(self.rows_amount):
            for column_n in range(self.columns_amount):
                self.blocks.append(
                    MapRectangle(
                        long1=self.map_rectangle.long1 + column_n * long_diff,
                        lat1=self.map_rectangle.lat1 + row_n * lat_diff,
                        long2=self.map_rectangle.long1 + (column_n + 1) * long_diff
                        if column_n != self.columns_amount - 1
                        else self.map_rectangle.long2,
                        lat2=self.map_rectangle.lat1 + (row_n + 1) * lat_diff
                        if row_n != self.rows_amount - 1
                        else self.map_rectangle.lat2,
                    )
                )

    def find_block(self, long: float, lat: float):
        for row_n in range(self.rows_amount):
            for column_n in range(self.columns_amount):
                if [long, lat] in self.blocks[row_n * self.columns_amount + column_n]:
                    return row_n * self.columns_amount + column_n


class ETL:
    def __init__(self, filename: str, blocks_number: int) -> None:
        self.filename: str = filename
        self.raw_data: dict = {}
        self.data = []
        self.prepared_data: list[dict] = []
        self.counter = 0
        self.blocks_number: int = blocks_number if blocks_number > 0 else 1
        self.table_factor: tuple[int] = calc_closest_factors(self.blocks_number)
        self.map_rectangle = MapRectangle()
        self.meta: list[dict] = []
        self.load_data()
        self.parse()
        self.map = Map(self.map_rectangle, self.table_factor[0], self.table_factor[1])
        self.data_to_send: dict[int, dict[int, list[float]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self.split_data()
        self.mq_manager = MQManager()
        for i in range(self.blocks_number):
            self.mq_manager.add_queue(f"worker_{i}")

    def send_all(self):
        self.mq_manager.send_message(
            "manager", {"command": COMMAND_META, "blocks_number": self.blocks_number, "meta": self.meta}
        )
        for i in range(self.blocks_number):
            self.mq_manager.send_message(f"worker_{i}", {"command": COMMAND_META, "data": self.data_to_send[i]})

    def load_data(self) -> None:
        with open(self.filename, "r") as f:
            self.raw_data = json.load(f)

    def parse(self) -> None:
        self.data: list[dict] = self.raw_data.get("features", [])
        for feature in self.data:
            if feature.get("type") != "Feature":
                continue
            if feature.get("geometry", {}).get("type", "") != "LineString":
                continue
            self.prepared_data.append(
                {
                    "type": "Feature",
                    "properties": {
                        "id": self.counter,
                        "name": feature.get("properties", {}).get("name", ""),
                    },
                    "geometry": feature["geometry"],
                }
            )
            feature_rectangle: MapRectangle = self._find_feature_rectangle(
                self.prepared_data[-1]["geometry"]["coordinates"]
            )
            self.map_rectangle.update_rectangle(feature_rectangle)
            self.counter += 1

    def split_data(self) -> None:
        for feature in self.prepared_data:
            self.meta.append(
                {
                    "id": feature["properties"]["id"],
                    "name": feature["properties"]["name"],
                    "parts_info": [],
                }
            )
            coords_count = 0
            previous_block_number: int = -1
            for coordinates in feature["geometry"]["coordinates"]:
                block_number: int = self.map.find_block(coordinates[0], coordinates[1])
                self.data_to_send[block_number][feature["properties"]["id"]].append(
                    coordinates
                )
                if previous_block_number != block_number:
                    if previous_block_number > -1:
                        self.meta[-1]["parts_info"].append(
                            [previous_block_number, coords_count]
                        )
                    coords_count = 1
                    previous_block_number = block_number
                    continue
                coords_count += 1
            self.meta[-1]["parts_info"].append([block_number, coords_count])

    def _find_feature_rectangle(self, data: list[list[float]]) -> MapRectangle:
        rectangle = MapRectangle(
            min(data, key=lambda x: x[0])[0],
            min(data, key=lambda x: x[1])[1],
            max(data, key=lambda x: x[0])[0],
            max(data, key=lambda x: x[1])[1],
        )
        return rectangle
