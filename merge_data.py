"""Merges data from the different datasets into one CSV file."""

import gzip
from typing import Generator, Dict, Tuple
import simplejson
from datetime import datetime
import re

# some timestamps include milliseconds, which are removed with this regex
TS_MILLIS_RE = re.compile(r"\.\d+Z")


def stream_gzipped(file: str) -> Generator[str, None, None]:
    """Read a gzipped text file line-by-line."""
    with gzip.open(file) as f:
        while (line := f.readline()) != b"":
            yield line.decode()


def parse_common(json: Dict) -> Dict:
    """Parse fields common to both commodity and journal events."""
    return {
        "system_name": json.pop("StarSystem", None) or json.pop("systemName"),
        "station_name": json.pop("StationName", None) or json.pop("stationName"),
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


def parse_journal(json):
    event = parse_common(json)


def combine_eddn_events(file: str):
    commodity = []
    journal = []

    with open(file) as f:
        for line in f:
            json = simplejson.loads(line)
            schema, msg = json["$schemaRef"], json["message"]

            if "commodity" in schema:
                commodity.append(parse_commodity(msg))
            elif "journal" in schema:
                journal.append(parse_journal(msg))

            if len(msg) != 0:
                raise ValueError(f"Extra fields! {json}")

    print(f"Read {len(commodity)} commodity and {len(journal)} journal events")

    return []


def main():
    events = combine_eddn_events("data/events.jsonl")


if __name__ == "__main__":
    main()
