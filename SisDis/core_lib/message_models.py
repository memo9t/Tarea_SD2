from __future__ import annotations
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from .kafka_client import JsonRecord


def _current_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _generate_trace_id() -> str:
    return uuid.uuid4().hex


@dataclass(slots=True)
class QuestionMessage(JsonRecord):
    message_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    trace_id: str = field(default_factory=_generate_trace_id)
    question_id: str = ""
    title: str = ""
    content: str = ""
    best_answer: str = ""
    source: str = "data-generator"
    created_at: str = field(default_factory=_current_iso)

    def asdict(self):
        return asdict(self)


@dataclass(slots=True)
class InquiryMessage(JsonRecord):
    message_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    trace_id: str = field(default_factory=_generate_trace_id)
    question_id: str = ""
    title: str = ""
    content: str = ""
    best_answer: str = ""
    source: str = "data-generator"
    created_at: str = field(default_factory=_current_iso)
    attempts: int = 0


@dataclass(slots=True)
class ModelRequest(JsonRecord):
    message_id: str
    trace_id: str
    question_id: str
    title: str
    content: str
    prompt: str
    best_answer: str
    attempts: int = 0
    created_at: str = field(default_factory=_current_iso)


@dataclass(slots=True)
class ModelResponse(JsonRecord):
    message_id: str
    trace_id: str
    question_id: str
    title: str
    content: str
    prompt: str
    llm_answer: str
    best_answer: str
    latency_ms: float
    attempts: int
    produced_at: str = field(default_factory=_current_iso)


@dataclass(slots=True)
class FailureMessage(JsonRecord):
    message_id: str
    trace_id: str
    question_id: str
    error_type: str
    error_detail: str
    attempts: int
    retry_at: str | None = None
    produced_at: str = field(default_factory=_current_iso)


@dataclass(slots=True)
class EvaluatedResult(JsonRecord):
    message_id: str
    trace_id: str
    question_id: str
    title: str
    content: str
    llm_answer: str
    best_answer: str
    score: float
    accepted: bool
    source: str
    attempts: int
    decided_at: str = field(default_factory=_current_iso)
