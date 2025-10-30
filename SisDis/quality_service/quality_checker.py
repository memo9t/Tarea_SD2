from __future__ import annotations
import argparse
import json
import socket
import threading
import time
from dataclasses import dataclass

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


@dataclass
class QualityRequest:
    question_id: str
    reference: str
    generated: str


def text_score(reference: str, generated: str) -> float:
    if not reference or not generated:
        return 0.0
    vec = TfidfVectorizer().fit([reference, generated])
    mat = vec.transform([reference, generated])
    score = cosine_similarity(mat[0], mat[1])[0][0]
    return max(0.0, min(1.0, float(score)))


def compute_quality(req: QualityRequest, threshold: float) -> dict[str, object]:
    score = text_score(req.reference, req.generated)
    accepted = score >= threshold
    return {"score": round(score, 4), "accepted": accepted}


def decode_request(raw: str) -> QualityRequest | None:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    try:
        return QualityRequest(
            question_id=str(payload["question_id"]),
            reference=payload.get("best_answer", ""),
            generated=payload.get("llm_answer", ""),
        )
    except KeyError:
        return None


def start_quality_server(host: str, port: int, threshold: float):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)
    print(f"Quality service active on {host}:{port} threshold={threshold}")

    def handle(conn: socket.socket):
        with conn:
            try:
                data = conn.recv(8192).decode().strip()
            except socket.error:
                return
            if not data:
                return
            req = decode_request(data)
            if not req:
                conn.sendall(json.dumps({"error": "invalid"}).encode() + b"\n")
                return
            start = time.time()
            result = compute_quality(req, threshold)
            result["elapsed_ms"] = round((time.time() - start) * 1000, 2)
            conn.sendall(json.dumps(result).encode() + b"\n")

    while True:
        cli, _ = server.accept()
        threading.Thread(target=handle, args=(cli,), daemon=True).start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=7000)
    parser.add_argument("--threshold", type=float, default=0.5)
    args = parser.parse_args()
    start_quality_server(args.host, args.port, args.threshold)
