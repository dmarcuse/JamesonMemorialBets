"""
Author: Garrett Powell & Dana Marcuse
Class: Machine Learning
Assignment: Final Project
Date: 04 March 2020

Certification of Authenticity:

I certify that this is entirely my own work, except where I have given fully-documented references to the work of
others. I understand the definition and consequences of plagiarism and acknowledge that the assessor of this assignment
may, for the purpose of assessing this assignment, reproduce this assignment and provide a copy to another member of
academic staff; and/or-Communicate a copy of this assignment to a plagiarism checking service (which may then retain a
copy of this assignment on its database for the purpose of future plagiarism checking).
"""
import dataclasses
import gzip
import json
from csv import DictWriter
from dataclasses import dataclass
from json import JSONDecodeError
from typing import Generator, Dict

SYSTEM_DATA = "data/systems_populated.json.gz"
CSV_FILE = "data/systems_populated.csv"


@dataclass
class System:
    # General information
    id: int
    security: str
    population: int
    stars: int

    # Planet types
    metal_bodies: int
    rock_bodies: int
    ice_bodies: int
    water_bodies: int
    gas_giants: int

    # Ring types
    rocky_rings: int
    icy_rings: int
    metal_rich_rings: int
    metallic_rings: int


def stream_json(file: str) -> Generator[Dict, None, None]:
    with gzip.open(file) as file:
        for json_line in iter(file.readline, b""):
            try:
                yield json.loads(json_line.decode("ascii").strip().rstrip(","))
            except JSONDecodeError:
                continue


with open(CSV_FILE, "w") as csv_file:
    field_names = [field.name for field in dataclasses.fields(System)]
    csv_writer = DictWriter(csv_file, field_names)
    csv_writer.writeheader()

    for json_object in stream_json(SYSTEM_DATA):

        stars = 0

        metal_bodies = 0
        rock_bodies = 0
        ice_bodies = 0
        gas_giants = 0
        water_bodies = 0

        rocky_rings = 0
        icy_rings = 0
        metal_rich_rings = 0
        metallic_rings = 0

        if "bodies" in json_object:
            for body in json_object["bodies"]:
                if body["type"] == "Star":
                    stars += 1
                    continue

                sub_type = body["subType"].lower()

                if "metal" in sub_type:
                    metal_bodies += 1
                elif "rocky" in sub_type:
                    rock_bodies += 1
                elif "icy" in sub_type:
                    ice_bodies += 1
                elif "gas giant" in sub_type:
                    gas_giants += 1
                elif "water" in sub_type or "ammonia" in sub_type or "earthlike" in sub_type:
                    water_bodies += 1
                    continue

                if "rings" in body:
                    for ring in body["rings"]:
                        ring_type = ring["type"].lower()

                        if ring_type == "Rocky":
                            rocky_rings += 1
                        elif ring_type == "Icy":
                            icy_rings += 1
                        elif ring_type == "Metal Rich":
                            metal_rich_rings += 1
                        elif ring_type == "Metallic":
                            metallic_rings += 1
                        else:
                            continue

        system_info = System(
            id=json_object["id"],
            security=json_object["security"],
            population=json_object["population"],
            stars=stars,
            metal_bodies=metal_bodies,
            rock_bodies=rock_bodies,
            ice_bodies=ice_bodies,
            water_bodies=water_bodies,
            gas_giants=gas_giants,
            rocky_rings=rocky_rings,
            icy_rings=icy_rings,
            metal_rich_rings=metal_rich_rings,
            metallic_rings=metallic_rings
        )

        csv_writer.writerow(dataclasses.asdict(system_info))
