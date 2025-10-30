from __future__ import annotations

import argparse
import logging
import os
import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from typing import Any

from core_lib.message_models import (
    ModelRequest,
    InquiryMessage,
    EvaluatedResult,
)
from core_lib.kafka_client import (
    build_consumer,
    build_producer,
    ensure_topics,
)
from core_lib import configure_logging
from core_lib.mongo_client import connect_to_mongo
from kafka.admin import NewTopic

LOG = logging.getLogger(__name__)


@dataclass
class Entry:
    value: dict[str, Any]
    stored_at: float


class CacheStore:
    def __init__(self, size: int, ttl: float):
        self.size = size
        self.ttl = ttl
        self.items: dict[str, Entry] = {}
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def put(self, key: str, value: dict[str, Any]) -> None:
        raise NotImplementedError

    def evict(self, key: str) -> None:
        self.items.pop(key, None)

    def is_expired(self, key: str) -> bool:
        if self.ttl <= 0 or key not in self.items:
            return False
        return time.time() - self.items[key].stored_at > self.ttl


class LRUStore(CacheStore):
    def __init__(self, size: int, ttl: float):
        super().__init__(size, ttl)
        self.order = OrderedDict()

    def evict(self, key: str) -> None:
        self.order.pop(key, None)
        super().evict(key)

    def get(self, key: str) -> dict[str, Any] | None:
        if key in self.items and not self.is_expired(key):
            self.order.move_to_end(key)
            self.hits += 1
            return self.items[key].value
        if key in self.items and self.is_expired(key):
            self.evict(key)
        self.misses += 1
        return None

    def put(self, key: str, value: dict[str, Any]) -> None:
        if key in self.items:
            self.order.move_to_end(key)
        else:
            if len(self.items) >= self.size:
                oldest, _ = self.order.popitem(last=False)
                super().evict(oldest)
            self.order[key] = None
        self.items[key] = Entry(value=value, stored_at=time.time())


class LFUStore(CacheStore):
    def __init__(self, size: int, ttl: float):
        super().__init__(size, ttl)
        self.freq = defaultdict(int)

    def evict(self, key: str) -> None:
        self.freq.pop(key, None)
        super().evict(key)

    def get(self, key: str) -> dict[str, Any] | None:
        if key in self.items and not self.is_expired(key):
            self.freq[key] += 1
            self.hits += 1
            return self.items[key].value
        if key in self.items and self.is_expired(key):
            self.evict(key)
        self.misses += 1
        return None

    def put(self, key: str, value: dict[str, Any]) -> None:
        if key not in self.items and len(self.items) >= self.size:
            least = min(self.freq, key=lambda item: (self.freq[item], self.items[item].stored_at))
            self.evict(least)
        self.items[key] = Entry(value=value, stored_at=time.time())
        self.freq[key] += 1


class FIFOStore(CacheStore):
    def __init__(self, size: int, ttl: float):
        super().__init__(size, ttl)
        self.queue: list[str] = []

    def evict(self, key: str) -> None:
        try:
            self.queue.remove(key)
        except ValueError:
            pass
        super().evict(key)

    def get(self, key: str) -> dict[str, Any] | None:
        if key in self.items and not self.is_expired(key):
            self.hits += 1
            return self.items[key].value
        if key in self.items and self.is_expired(key):
            self.evict(key)
        self.misses += 1
        return None

    def put(self, key: str, value: dict[str, Any]) -> None:
        if key not in self.items:
            if len(self.items) >= self.size and self.queue:
                oldest = self.queue.pop(0)
                super().evict(oldest)
            self.queue.append(key)
        self.items[key] = Entry(value=value, stored_at=time.time())


def make_cache(policy: str, size: int, ttl: float) -> CacheStore:
    if policy == "lru":
        return LRUStore(size, ttl)
    if policy == "lfu":
        return LFUStore(size, ttl)
    if policy == "fifo":
        return FIFOStore(size, ttl)
    raise ValueError(policy)


class BufferHandler:
    def __init__(
        self,
        *,
        policy: str,
        size: int,
        ttl: float,
        mongo_uri: str,
        database: str,
        collection: str,
        input_topic: str,
        llm_topic: str,
        validated_topic: str,
        regeneration_topic: str,
        group_id: str,
    ):
        self.cache = make_cache(policy, size, ttl)
        client = connect_to_mongo(mongo_uri)
        self.store = client[database][collection]
        self.consumer = build_consumer("", group_id=group_id)
        self.consumer.subscribe([input_topic, regeneration_topic])
        self.producer = build_producer()
        self.llm_topic = llm_topic
        self.validated_topic = validated_topic
        self.regen_topic = regeneration_topic

    def storage_lookup(self, question_id: str) -> dict[str, Any] | None:
        record = self.store.find_one({"id_pregunta": question_id})
        if not record or not record.get("respuesta_llm"):
            return None
        return {
            "question_id": question_id,
            "llm_answer": record.get("respuesta_llm", ""),
            "best_answer": record.get("respuesta_dataset", ""),
            "score": float(record.get("score", 0.0)),
            "accepted": bool(record.get("aceptado", False)),
            "title": record.get("pregunta", ""),
            "content": record.get("contenido", ""),
        }

    def send_validated(self, msg: InquiryMessage, cached: dict[str, Any], src: str) -> None:
        r = EvaluatedResult(
            message_id=msg.message_id,
            trace_id=msg.trace_id,
            question_id=msg.question_id,
            title=cached.get("title", msg.title),
            content=cached.get("content", msg.content),
            llm_answer=cached.get("llm_answer", ""),
            best_answer=cached.get("best_answer", msg.best_answer),
            score=float(cached.get("score", 0.0)),
            accepted=bool(cached.get("accepted", True)),
            source=src,
            attempts=msg.attempts,
        )
        self.producer.send(self.validated_topic, key=r.message_id, value=r.asdict())
        self.producer.flush()

    def send_llm_request(self, msg: InquiryMessage) -> None:
        parts = [msg.title, msg.content]
        text = "\n\n".join(p for p in parts if p)
        req = ModelRequest(
            message_id=msg.message_id,
            trace_id=msg.trace_id,
            question_id=msg.question_id,
            title=msg.title,
            content=msg.content,
            prompt=text,
            best_answer=msg.best_answer,
            attempts=msg.attempts + 1,
        )
        self.producer.send(self.llm_topic, key=req.message_id, value=req.asdict())
        self.producer.flush()

    def run(self) -> None:
        LOG.info("Buffer listo")
        while True:
            records = self.consumer.poll(timeout_ms=1000)
            for _, messages in records.items():
                for rec in messages:
                    data = rec.value
                    if rec.topic == self.regen_topic:
                        attempts = int(data.get("attempts", 0))
                        req = ModelRequest(
                            message_id=data["message_id"],
                            trace_id=data["trace_id"],
                            question_id=data["question_id"],
                            title=data.get("title", ""),
                            content=data.get("content", ""),
                            prompt=data.get("prompt", ""),
                            best_answer=data.get("best_answer", ""),
                            attempts=attempts + 1,
                        )
                        self.producer.send(self.llm_topic, key=req.message_id, value=req.asdict())
                        self.producer.flush()
                        continue

                    msg = InquiryMessage(**data)
                    cached = self.cache.get(msg.question_id)
                    if cached:
                        self.send_validated(msg, cached, "cache")
                    else:
                        found = self.storage_lookup(msg.question_id)
                        if found:
                            self.cache.put(msg.question_id, found)
                            self.send_validated(msg, found, "storage")
                        else:
                            self.send_llm_request(msg)
                if messages:
                    self.consumer.commit()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy", default="lru", choices=["lru", "lfu", "fifo"])
    parser.add_argument("--size", type=int, default=1024)
    parser.add_argument("--ttl", type=float, default=3600)
    parser.add_argument("--mongo-uri", default="mongodb://mongo:27017/")
    parser.add_argument("--mongo-db", default="yahoo_db")
    parser.add_argument("--mongo-coll", default="results")
    parser.add_argument("--input-topic", default="questions_in")
    parser.add_argument("--llm-topic", default="llm_requests")
    parser.add_argument("--validated-topic", default="validated_responses")
    parser.add_argument("--regeneration-topic", default="regeneration_requests")
    parser.add_argument("--group-id", default="buffer-service")
    args = parser.parse_args()

    configure_logging()

    partitions = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "1"))
    replication = int(os.getenv("KAFKA_TOPIC_REPLICATION", "1"))

    ensure_topics(
        [
            NewTopic(name=args.llm_topic, num_partitions=partitions, replication_factor=replication),
            NewTopic(name=args.validated_topic, num_partitions=partitions, replication_factor=replication),
            NewTopic(name=args.regeneration_topic, num_partitions=partitions, replication_factor=replication),
        ]
    )

    service = BufferHandler(
        policy=args.policy,
        size=args.size,
        ttl=args.ttl,
        mongo_uri=args.mongo_uri,
        database=args.mongo_db,
        collection=args.mongo_coll,
        input_topic=args.input_topic,
        llm_topic=args.llm_topic,
        validated_topic=args.validated_topic,
        regeneration_topic=args.regeneration_topic,
        group_id=args.group_id,
    )
    service.run()


if __name__ == "__main__":
    main()
