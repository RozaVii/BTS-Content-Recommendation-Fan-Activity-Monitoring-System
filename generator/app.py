import json
import random
import time
from datetime import datetime

import redis
from faker import Faker
import numpy as np

fake = Faker()

# Redis connection
redis_client = redis.Redis(
    host="redis",
    port=6379,
    decode_responses=True
)

STREAM_NAME = "bts_fan_events"

# BTS-related constants
MEMBERS = ["RM", "Jin", "Suga", "J-Hope", "Jimin", "V", "Jungkook"]
CONTENT_TYPES = [
    "music_video",
    "song",
    "album",
    "interview",
    "live_concert",
    "behind_the_scenes"
]
EVENT_TYPES = ["view", "like", "add_to_favorites", "share", "skip"]
SOURCES = ["youtube", "vlive", "weverse", "spotify"]

# Mode: normal / comeback / anomaly
MODE = "normal"

def generate_event():
    member_bias = random.choice(MEMBERS)

    event_type = random.choices(
        EVENT_TYPES,
        weights=[0.6, 0.15, 0.1, 0.05, 0.1],
        k=1
    )[0]

    watch_time = int(
        max(5, np.random.normal(loc=120, scale=40))
    )

    if MODE == "anomaly":
        event_type = "skip"
        watch_time = random.randint(1, 5)

    event = {
        "event_time": datetime.utcnow().isoformat(),
        "user_id": random.randint(1, 50_000),
        "content_id": random.randint(1, 500),
        "content_type": random.choice(CONTENT_TYPES),
        "member_bias": member_bias,
        "event_type": event_type,
        "watch_time_sec": watch_time,
        "source": random.choice(SOURCES),
        "mode": MODE
    }

    return event


def main():
    print("BTS Fan Event Generator started")

    while True:
        event = generate_event()

        redis_client.xadd(
            STREAM_NAME,
            {"data": json.dumps(event)}
        )

        print(f"Sent event: {event['event_type']} | {event['member_bias']}")

        # Load control
        if MODE == "comeback":
            time.sleep(0.2)   # high load
        else:
            time.sleep(1)


if __name__ == "__main__":
    main()