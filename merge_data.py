"""Merges data from the different datasets into one CSV file."""

import gzip
from typing import Generator


def stream_gzipped(file: str) -> Generator[str, None, None]:
    with gzip.open(file) as f:
        while (line := f.readline()) != b"":
            yield line.decode("ascii")


def main():
    count = 0
    for line in stream_gzipped("data/systems_populated.json.gz"):
        count += 1

    print(f"Read {count} rows")


if __name__ == "__main__":
    main()
