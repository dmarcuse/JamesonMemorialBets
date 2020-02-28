"""Data collector using EDDN to collect Elite: Dangerous events."""

import zlib
import zmq
import simplejson
import time

relayURI = "tcp://eddn.edcd.io:9500"
outputFile = "data/events.jsonl"


def should_save(event) -> bool:
    """Check whether the given event should be saved."""
    schema = event["$schemaRef"]
    if schema == "https://eddn.edcd.io/schemas/commodity/3":
        return True
    elif schema == "https://eddn.edcd.io/schemas/journal/1":
        return event["message"]["event"] == "Docked"


def main():
    """Connect to EDDN and save relevant events."""
    # initialize zeromq subscriber
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)

    # subscribe to all events
    subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    subscriber.setsockopt(zmq.RCVTIMEO, 600000)

    file = open(outputFile, "a")
    events = 0

    while True:
        try:
            subscriber.connect(relayURI)

            while True:
                msg = subscriber.recv()

                if not msg:
                    subscriber.disconnect(relayURI)
                    break

                msg = zlib.decompress(msg)
                json = simplejson.loads(msg)
                if should_save(json):
                    file.write(msg.decode("utf8") + "\n")
                    events += 1
                    print(f"Received {events} events", end="\r")
        except zmq.ZMQError as e:
            print(f"ZMQ error: {e}")
            subscriber.disconnect(relayURI)
            time.sleep(5)


if __name__ == "__main__":
    main()
