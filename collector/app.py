import json
import time
from datetime import datetime

import redis
import clickhouse_connect

# Для имитации очереди сообщений в реальном времени
redis_client = redis.Redis(
    host="redis",
    port=6379,
    decode_responses=True
)

STREAM_NAME = "bts_fan_events"
GROUP_NAME = "bts_consumers"
CONSUMER_NAME = "collector_1"
#Читает и записывает в бд

# Отдаем данные в бд
ch_client = clickhouse_connect.get_client(
    host="clickhouse",
    port=8123,
    username="default",
    password=""
)

def create_consumer_group():
    try:
        redis_client.xgroup_create(
            STREAM_NAME,
            GROUP_NAME,
            id="0",
            mkstream=True
        )
        print("Consumer group created")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print("Consumer group already exists")
        else:
            raise

def process_event(event):
    data = json.loads(event)

    ch_client.insert(
        table="bts.fan_events",
        data=[[
            datetime.fromisoformat(data["event_time"]),
            data["event_id"],
            data["user_id"],
            data["content_id"],
            data["content_type"],
            data["member_bias"],
            data["event_type"],
            data["watch_time_sec"],
            data["source"],
            data["mode"]
        ]],
        column_names=[
            "event_time",
            "event_id",
            "user_id",
            "content_id",
            "content_type",
            "member_bias",
            "event_type",
            "watch_time_sec",
            "source",
            "mode"
        ]
    )

def main():
    print("Collector started")
    create_consumer_group()

    while True:
        messages = redis_client.xreadgroup(
            GROUP_NAME,
            CONSUMER_NAME,
            {STREAM_NAME: ">"},
            count=10,
            block=5000
        )

        if not messages:
            continue

        for stream, events in messages:
            for message_id, fields in events:
                try:
                    process_event(fields["data"])
                    redis_client.xack(
                        STREAM_NAME,
                        GROUP_NAME,
                        message_id
                    )
                    print(f"Processed event {message_id}")
                except Exception as e:
                    print(f"Error processing {message_id}: {e}")

        time.sleep(0.1)

if __name__ == "__main__":
    main()
