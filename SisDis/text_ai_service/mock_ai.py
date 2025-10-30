from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import threading
import time
from dataclasses import dataclass
from typing import Optional

import google.generativeai as genai


class RequestRateLimiter:
    """Control de tasa simple para limitar solicitudes procesadas por segundo."""

    def __init__(self, max_rps: float | None = None):
        self.min_interval = 1.0 / max_rps if max_rps else 0.0
        self._lock = threading.Lock()
        self._last_call = 0.0

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        with self._lock:
            now = time.time()
            wait_time = self.min_interval - (now - self._last_call)
            if wait_time > 0:
                time.sleep(wait_time)
                now = time.time()
            self._last_call = now


class ModelClientError(RuntimeError):
    """Errores relacionados con el servicio del modelo."""


@dataclass(slots=True)
class ModelClientConfig:
    api_key: Optional[str]
    model_name: str = "gemini-1.5-flash"
    timeout: float = 5.0
    retries: int = 1
    backoff: float = 2.0
    offline_mode: bool = False
    fallback_enabled: bool = True


class LocalTextModelClient:
    """
    Cliente simple que puede responder mediante Gemini u offline si falla.
    """

    def __init__(self, config: ModelClientConfig):
        self._config = config
        self._offline = config.offline_mode
        self._fallback = config.fallback_enabled
        self._model: Optional[genai.GenerativeModel] = None
        self._lock = threading.Lock()

        if not self._offline:
            if not config.api_key:
                raise ValueError("Se requiere una API key para usar el modelo remoto.")
            genai.configure(api_key=config.api_key)
            self._model = genai.GenerativeModel(config.model_name)
        else:
            print("[MockAI] Ejecutando en modo local.", file=sys.stderr)

    def make_prompt(self, question: str, additional: str) -> str:
        return (
            "Tu tarea es responder una pregunta con claridad.\n"
            f"Pregunta: {question or '(Sin pregunta)'}\n\n"
            f"Detalles: {additional or '(Sin detalles)'}\n\n"
            "Respuesta:"
        )

    def answer_from_model(self, question: str, details: str) -> str:
        prompt = self.make_prompt(question, details)
        last_error: Optional[Exception] = None

        if self._offline:
            return self._local_fallback(question, details, None)

        for n in range(1, self._config.retries + 1):
            try:
                with self._lock:
                    assert self._model is not None
                    response = self._model.generate_content(
                        prompt,
                        request_options={"timeout": self._config.timeout},
                    )
                text = getattr(response, "text", "").strip()
                if not text:
                    raise ModelClientError("Respuesta vacía desde el modelo remoto.")
                return text
            except Exception as exc:
                last_error = exc
                if self._fallback:
                    print(
                        f"[MockAI] Error en intento {n}: {exc}. Usando fallback.",
                        file=sys.stderr,
                    )
                    break
                if n >= self._config.retries:
                    break
                time.sleep(self._config.backoff * n)

        if self._fallback:
            return self._local_fallback(question, details, last_error)
        raise ModelClientError(str(last_error) if last_error else "Error desconocido.")

    def _local_fallback(self, question: str, details: str, error: Optional[Exception]) -> str:
        msg = (
            f"Pregunta: {question}\n\n"
            f"Detalles: {details}\n\n"
            "Respuesta generada localmente:\n"
            "- Considera analizar la pregunta por pasos.\n"
            "- Verifica la respuesta con otras fuentes.\n"
        )
        if error:
            msg += f"\n(Fallo remoto: {error})"
        return msg


def launch_mock_server(
    host: str,
    port: int,
    max_rps: float,
    max_concurrent: int,
    timeout: float,
    config: ModelClientConfig,
):
    limiter = RequestRateLimiter(max_rps if max_rps > 0 else None)
    semaphore = threading.Semaphore(max_concurrent if max_concurrent > 0 else 1)
    model_client = LocalTextModelClient(config)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)

    print(f"[MockAI] Servidor activo en {host}:{port}", file=sys.stderr)

    def handle(conn: socket.socket):
        with conn, semaphore:
            conn.settimeout(timeout)
            try:
                data = conn.recv(4096).decode().strip()
            except socket.timeout:
                conn.sendall(json.dumps({"error": "timeout"}).encode() + b"\n")
                return

            if not data:
                return

            try:
                payload = json.loads(data)
            except json.JSONDecodeError:
                conn.sendall(json.dumps({"error": "invalid_json"}).encode() + b"\n")
                return

            limiter.wait()

            title = payload.get("title", "")
            content = payload.get("content", "")

            try:
                answer = model_client.answer_from_model(title, content)
                response = {"generated_answer": answer}
            except ModelClientError as exc:
                response = {"error": str(exc)}

            conn.sendall(json.dumps(response).encode() + b"\n")

    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle, args=(conn,), daemon=True).start()


def env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Servicio de modelo de texto")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=6000)
    parser.add_argument("--max_rps", type=float, default=5.0)
    parser.add_argument("--max_concurrent", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=5.0)

    parser.add_argument("--model_name", default=os.getenv("MODEL_NAME", "gemini-1.5-flash"))
    parser.add_argument("--model_timeout", type=float, default=float(os.getenv("MODEL_TIMEOUT", "15")))
    parser.add_argument("--model_retries", type=int, default=int(os.getenv("MODEL_RETRIES", "3")))
    parser.add_argument("--model_backoff", type=float, default=float(os.getenv("MODEL_BACKOFF", "2.0")))
    parser.add_argument("--model_api_key", default=os.getenv("MODEL_API_KEY"))

    parser.add_argument("--offline", action="store_true", help="Modo sin acceso externo")
    parser.add_argument("--no-offline", dest="offline", action="store_false")

    parser.add_argument("--fallback", action="store_true")
    parser.add_argument("--no-fallback", dest="fallback", action="store_false")

    parser.set_defaults(
        offline=env_flag("MODEL_OFFLINE", False),
        fallback=env_flag("MODEL_FALLBACK", True),
    )

    opts = parser.parse_args()

    config = ModelClientConfig(
        api_key=opts.model_api_key,
        model_name=opts.model_name,
        timeout=opts.model_timeout,
        retries=opts.model_retries,
        backoff=opts.model_backoff,
        offline_mode=opts.offline,
        fallback_enabled=opts.fallback,
    )

    launch_mock_server(
        opts.host, opts.port, opts.max_rps, opts.max_concurrent, opts.timeout, config
    )
