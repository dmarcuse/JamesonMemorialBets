"""Merges data from the different datasets into one CSV file."""

import json
import re
from datetime import datetime
from typing import Dict
from itertools import groupby
from csv import DictReader, DictWriter

# some timestamps include milliseconds, which are removed with this regex
TS_MILLIS_RE = re.compile(r"\.\d+Z")

# max time difference between events to be paired
MAX_EVENT_SEPARATION = 300


def parse_common(json: Dict) -> Dict:
    """Parse fields common to both commodity and journal events."""
    return {
        "system_name": json.pop("StarSystem", None) or json.pop("systemName"),
        "station": json.pop("StationName", None) or json.pop("stationName"),
        "market_id": json.pop("MarketID", None) or json.pop("marketId"),
        "timestamp": datetime.strptime(TS_MILLIS_RE.sub("Z", json.pop("timestamp")), "%Y-%m-%dT%H:%M:%SZ")  # ISO 8601
    }


def parse_commodity(json: Dict) -> Dict:
    event = parse_common(json)

    event["commodities"] = [
        {
            "buy_price": c.pop("buyPrice"),
            "demand": c.pop("demand"),
            "demand_bracket": c.pop("demandBracket"),
            "mean_price": c.pop("meanPrice"),
            "name": c.pop("name"),
            "sell_price": c.pop("sellPrice"),
            "stock": c.pop("stock"),
            "stock_bracket": c.pop("stockBracket"),
        } for c in json.pop("commodities")
    ]

    # unused fields
    json.pop("economies", None)
    json.pop("prohibited", None)
    json.pop("horizons", None)

    return event


economy_names = [
    "$economy_Agri;",
    "$economy_Colony;",
    "$economy_Extraction;",
    "$economy_HighTech;",
    "$economy_Industrial;",
    "$economy_Military;",
    "$economy_None;",
    "$economy_Refinery;",
    "$economy_Service;",
    "$economy_Terraforming;",
    "$economy_Tourism;",
    "$economy_Prison;",
    "$economy_Damaged;",
    "$economy_Rescue;",
    "$economy_Repair;"
]

service_names = [
    "Dock",
    "Autodock",
    "BlackMarket",
    "Commodities",
    "Contacts",
    "Exploration",
    "Initiatives",
    "Missions",
    "Outfitting",
    "CrewLounge",
    "Rearm",
    "Refuel",
    "Repair",
    "Shipyard",
    "Tuning",
    "Workshop",
    "MissionsGenerated",
    "Facilitator",
    "Research",
    "FlightController",
    "StationOperations",
    "OnDockMission",
    "Powerplay",
    "SearchAndRescue",
    "Shop",
    "MaterialTrader",
    "TechBroker"
]


def parse_journal(json_event: Dict) -> Dict:
    event = parse_common(json_event)

    # Miscellaneous station info
    event["station_allegiance"] = json_event.pop("StationAllegiance", "Independent")
    event["station_government"] = json_event.pop("StationGovernment")
    event["station_type"] = json_event.pop("StationType")
    event["faction_state"] = json_event.pop("StationFaction").pop("FactionState", "None")

    # Station economies
    default_economies = {name: 0.0 for name in economy_names}
    economies = {
        economy_json.pop("Name"): float(economy_json.pop("Proportion"))
        for economy_json in json_event.pop("StationEconomies")
    }
    event.update(default_economies)
    event.update(economies)

    # Station services
    default_services = {name: 0 for name in service_names}
    services = {service_name: 1 for service_name in json_event.pop("StationServices")}
    event.update(default_services)
    event.update(services)

    # Remove unused fields
    json_event.pop("SystemAddress", None)
    json_event.pop("CockpitBreach", None)
    json_event.pop("StationEconomy", None)
    json_event.pop("DistFromStarLS", None)
    json_event.pop("Wanted", None)
    json_event.pop("ActiveFine", None)
    json_event.pop("StarPos", None)
    json_event.pop("event", None)
    json_event.pop("Body", None)
    json_event.pop("BodyType", None)
    json_event.pop("StationState", None)

    return event


def timediff(a: datetime, b: datetime) -> float:
    return abs((a - b).total_seconds())


def combine_eddn_events(file: str):
    commodity = []
    journal = []

    with open(file) as f:
        for line in f:
            json_event = json.loads(line)
            schema, msg = json_event["$schemaRef"], json_event["message"]

            if "commodity" in schema:
                commodity.append(parse_commodity(msg))
            elif "journal" in schema:
                journal.append(parse_journal(msg))

            if len(msg) != 0:
                raise ValueError(f"Extra fields! {msg}")

    print(f"Read {len(commodity)} commodity and {len(journal)} journal events")

    # organize journal events into dictionary mapping system -> list of events
    journal.sort(key=lambda e: e["system_name"])
    journal_map = {}
    for k, g in groupby(journal, lambda e: e["system_name"]):
        journal_map[k] = sorted(g, key=lambda e: e["timestamp"])

    # load static system data
    with open("data/systems_populated.csv") as f:
        systems = {system["name"]: system for system in DictReader(f)}

    # combine all the data
    merged = []
    for c in commodity:
        # select events from the same station that are close enough in time
        matches = [
            j for j in journal_map.get(c["system_name"], [])
            if c["station"] == j["station"] and timediff(c["timestamp"], j["timestamp"]) <= MAX_EVENT_SEPARATION
        ]

        if matches:
            closest_journal = min(matches, key=lambda j: timediff(j["timestamp"], c["timestamp"]))
            merged += [
                {
                    **{k: v for k, v in c.items() if k != "commodities"},
                    **item,
                    **closest_journal,
                    **systems[c["system_name"]]
                } for item in c["commodities"]
            ]

    return merged


def main():
    events = combine_eddn_events("data/events.jsonl")
    with open("data/merged_data.csv", "w") as f:
        w = DictWriter(f, events[0])
        w.writeheader()
        w.writerows(events)


if __name__ == "__main__":
    main()
