import json
import random
import time
from datetime import datetime
import uuid

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
    source = random.choice(SOURCES)

    # Вероятности действий фанатов
    event_type = random.choices(
        EVENT_TYPES,
        weights=[0.55, 0.2, 0.1, 0.05, 0.1],
        k=1
    )[0]

    # Watch time логически зависит от события
    if event_type == "skip":
        watch_time = random.randint(1, 10)
    elif event_type == "view":
        watch_time = int(max(10, np.random.normal(120, 40)))
    else:
        watch_time = int(max(20, np.random.normal(180, 60)))

    # Аномальный режим
    if MODE == "anomaly":
        event_type = "skip"
        watch_time = random.randint(1, 5)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.utcnow().isoformat(),
        "user_id": random.randint(1, 50_000),
        "content_id": random.randint(1, 500),
        "content_type": random.choice(CONTENT_TYPES),
        "member_bias": member_bias,
        "event_type": event_type,
        "watch_time_sec": watch_time,
        "source": source,
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

        print(
            f"Sent event: {event['event_type']} | "
            f"{event['member_bias']} | "
            f"{event['source']} | "
            f"mode={event['mode']}"
        )

        # Load control
        if MODE == "normal":
            time.sleep(1)
        elif MODE == "comeback":
            time.sleep(0.2)
        elif MODE == "anomaly":
            time.sleep(0.05)


if __name__ == "__main__":
    main()
