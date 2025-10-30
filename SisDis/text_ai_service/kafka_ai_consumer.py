from __future__ import annotations
import argparse
import logging
import random
import time
from dataclasses import dataclass
from typing import Optional

from core_lib.message_models import FailureMessage, ModelRequest, ModelResponse
from core_lib.kafka_client import build_consumer, build_producer
from core_lib.logger_utils import configure_logging

LOG = logging.getLogger(__name__)


@dataclass(slots=True)
class RetryPolicy:
    max_retries: int = 5
    base_delay: float = 1.0
    jitter: float = 0.5


class SimpleModel:
    def __init__(self, latency_ms: float = 250.0):
        self.latency_ms = latency_ms

    def infer(self, prompt: str) -> str:
        time.sleep(self.latency_ms / 1000.0)
        summary = " ".join(prompt.split()[:60])
        return f"Respuesta generada automáticamente. Resumen: {summary}."


class ModelConsumer:
    def __init__(
        self,
        *,
        request_topic: str,
        response_topic: str,
        error_topic: str,
        group_id: str,
        retry_policy: RetryPolicy,
        model: Optional[SimpleModel] = None,
    ):
        self.consumer = build_consumer(request_topic, group_id=group_id)
        self.producer = build_producer()
        self.response_topic = response_topic
        self.error_topic = error_topic
        self.retry_policy = retry_policy
        self.model = model or SimpleModel()

    def _send_response(self, req: ModelRequest, answer: str, latency_ms: float) -> None:
        response = ModelResponse(
            message_id=req.message_id,
            trace_id=req.trace_id,
            question_id=req.question_id,
            title=req.title,
            content=req.content,
            prompt=req.prompt,
            llm_answer=answer,
            best_answer=req.best_answer,
            latency_ms=latency_ms,
            attempts=req.attempts,
        )
        self.producer.send(self.response_topic, key=response.message_id, value=response.asdict())
        self.producer.flush()

    def _send_error(self, req: ModelRequest, exc: Exception, attempt: int) -> None:
        delay = self.retry_policy.base_delay * (2 ** (attempt - 1))
        delay += random.uniform(0, self.retry_policy.jitter)
        retry_at = time.time() + delay
        error = FailureMessage(
            message_id=req.message_id,
            trace_id=req.trace_id,
            question_id=req.question_id,
            error_type=exc.__class__.__name__,
            error_detail=str(exc),
            attempts=attempt,
            retry_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(retry_at)),
        )
        self.producer.send(self.error_topic, key=error.message_id, value=error.asdict())
        self.producer.flush()

    def run(self) -> None:
        LOG.info("Text AI Consumer iniciado y escuchando mensajes...")
        while True:
            records = self.consumer.poll(timeout_ms=1000)
            for _, messages in records.items():
                for record in messages:
                    payload = record.value
                    req = ModelRequest(**payload)
                    start = time.time()
                    try:
                        answer = self.model.infer(req.prompt)
                    except Exception as exc:
                        LOG.exception("Error generando respuesta")
                        self._send_error(req, exc, req.attempts)
                    else:
                        latency = (time.time() - start) * 1000
                        self._send_response(req, answer, latency)
                if messages:
                    self.consumer.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Consumidor Kafka para modelo de texto")
    parser.add_argument("--request-topic", default="llm_requests")
    parser.add_argument("--response-topic", default="llm_responses")
    parser.add_argument("--error-topic", default="llm_errors")
    parser.add_argument("--group-id", default="text-model-consumer")
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--base-delay", type=float, default=1.0)
    parser.add_argument("--jitter", type=float, default=0.5)
    parser.add_argument("--latency", type=float, default=250.0)
    args = parser.parse_args()

    configure_logging()

    consumer = ModelConsumer(
        request_topic=args.request_topic,
        response_topic=args.response_topic,
        error_topic=args.error_topic,
        group_id=args.group_id,
        retry_policy=RetryPolicy(
            max_retries=args.max_retries,
            base_delay=args.base_delay,
            jitter=args.jitter,
        ),
        model=SimpleModel(latency_ms=args.latency),
    )
    consumer.run()


if __name__ == "__main__":
    main()
