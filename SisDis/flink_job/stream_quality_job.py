from __future__ import annotations

import json
import os
from dataclasses import dataclass

from kafka.admin import NewTopic
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import (
    KeyedProcessFunction,
    OutputTag,
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors.kafka import (
    FlinkKafkaConsumer,
    FlinkKafkaProducer,
)
from pyflink.datastream.state import ValueState, ValueStateDescriptor

from core_lib.message_models import EvaluatedResult
from core_lib.kafka_client import ensure_topics



@dataclass
class EvalConfig:
    threshold: float = 0.6
    limit: int = 3


def score_text(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    v = TfidfVectorizer().fit([a, b])
    m = v.transform([a, b])
    return max(0.0, min(1.0, float(cosine_similarity(m[0], m[1])[0][0])))


class EvalProcess(KeyedProcessFunction):
    def __init__(self, cfg: EvalConfig, regen_tag: OutputTag):
        super().__init__()
        self.cfg = cfg
        self.regen_tag = regen_tag
        self.state: ValueState | None = None
        self.ok = None
        self.reg = None

    def open(self, ctx):
        desc = ValueStateDescriptor("n", Types.INT())
        self.state = ctx.get_state(desc)
        mg = ctx.get_metric_group()
        self.ok = mg.counter("ok")
        self.reg = mg.counter("reg")

    def process_element(self, raw, ctx):
        data = json.loads(raw)
        n = (self.state.value() or 0) + 1
        self.state.update(n)

        score = score_text(data.get("best_answer", ""), data.get("llm_answer", ""))
        good = score >= self.cfg.threshold or n >= self.cfg.limit

        val = ValidatedResponse(
            message_id=data["message_id"],
            trace_id=data["trace_id"],
            question_id=data["question_id"],
            title=data.get("title", ""),
            content=data.get("prompt", ""),
            llm_answer=data.get("llm_answer", ""),
            best_answer=data.get("best_answer", ""),
            score=score,
            accepted=good,
            source="stream",
            attempts=n,
        )

        if good:
            self.ok.inc()
            self.state.clear()
            yield json.dumps(val.asdict())
        else:
            self.reg.inc()
            ctx.output(
                self.regen_tag,
                json.dumps(
                    {
                        "message_id": data["message_id"],
                        "trace_id": data["trace_id"],
                        "question_id": data["question_id"],
                        "title": data.get("title", ""),
                        "content": data.get("content", ""),
                        "prompt": data.get("prompt", ""),
                        "best_answer": data.get("best_answer", ""),
                        "attempts": n,
                    }
                ),
            )


def kafka_in(topic: str) -> FlinkKafkaConsumer:
    props = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "group.id": os.getenv("FLINK_GROUP_ID", "flink-job"),
        "auto.offset.reset": "earliest",
    }
    return FlinkKafkaConsumer(topic, SimpleStringSchema(), props)


def kafka_out(topic: str) -> FlinkKafkaProducer:
    props = {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")}
    return FlinkKafkaProducer(topic, SimpleStringSchema(), props)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "1")))

    cfg = EvalConfig(
        threshold=float(os.getenv("SCORE_THRESHOLD", "0.6")),
        limit=int(os.getenv("MAX_RETRIES", "3")),
    )

    rep = int(os.getenv("KAFKA_TOPIC_REPLICATION", "1"))
    parts = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "1"))
    topic_in = os.getenv("LLM_RESPONSES_TOPIC", "llm_responses")
    topic_out = os.getenv("VALIDATED_TOPIC", "validated_responses")
    topic_reg = os.getenv("REGEN_TOPIC", "regeneration_requests")

    ensure_topics(
        [
            NewTopic(topic_in, parts, rep),
            NewTopic(topic_out, parts, rep),
            NewTopic(topic_reg, parts, rep),
        ]
    )

    src = env.add_source(kafka_in(topic_in))

    regen = OutputTag("regen", Types.STRING())

    proc = (
        src.assign_timestamps_and_watermarks(WatermarkStrategy.for_monotonous_timestamps())
        .key_by(lambda r: json.loads(r)["message_id"], key_type=Types.STRING())
        .process(EvalProcess(cfg, regen))
    )

    proc.add_sink(kafka_out(topic_out))
    proc.get_side_output(regen).add_sink(kafka_out(topic_reg))

    env.execute("stream-quality-job")


if __name__ == "__main__":
    main()
