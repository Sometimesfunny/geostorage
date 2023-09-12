import json
from typing import Any


def to_json(data: Any) -> str:
    return json.dumps(data)


def from_json(data: str):
    return json.loads(data)


def calc_closest_factors(x: int) -> tuple[int]:
    if not isinstance(x, int):
        raise TypeError("x must be an integer.")

    a, b, i = 1, x, 0
    while a < b:
        i += 1
        if x % i == 0:
            a = i
            b = x // a

    return (b, a)
