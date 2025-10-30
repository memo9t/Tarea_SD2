from __future__ import annotations
import argparse
import csv
import random
import sys
import time
from datetime import datetime
from pathlib import Path

import matplotlib
import pandas as pd
from kafka.admin import NewTopic

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))


from core_lib.message_models import QuestionMessage
from core_lib.message_models import InquiryMessage
from core_lib.kafka_client import build_producer, ensure_topics
from core_lib import configure_logging

from core_lib.mongo_client import connect_to_mongo

matplotlib.use("Agg")


def p_inter(l: float) -> float:
    return random.expovariate(l)


def u_inter(a: float, b: float) -> float:
    return random.uniform(a, b)


def mk_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def clean_part(v: object) -> str:
    return str(v).replace(" ", "-").replace("/", "-").replace(".", "_")


def mk_logname(dis: str, total: int, prm: dict[str, float], out: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    parts = ["prod", dis, f"n{total}"]
    if dis == "poisson":
        parts.append(f"l{clean_part(prm['lmbda'])}")
    else:
        parts.append(f"a{clean_part(prm['low'])}")
        parts.append(f"b{clean_part(prm['high'])}")
    parts.append(stamp)
    mk_dir(out)
    return out / ("_".join(parts) + ".csv")


def log_csv(file: Path, rows: list[list[object]]) -> None:
    hdr = not file.exists()
    with file.open(mode="a", newline="", encoding="utf-8") as h:
        w = csv.writer(h)
        if hdr:
            w.writerow(["ts", "op", "mid", "qid", "st", "lat", "topic"])
        w.writerows(rows)


def mk_graphs(file: Path, out: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows)
    if df.empty:
        return
    gdir = mk_dir(out / "plots")
    base = file.stem
    cnt = df["st"].value_counts().sort_index()
    if not cnt.empty:
        ax = cnt.plot(kind="bar", title="Publicados")
        ax.set_xlabel("Estado")
        ax.set_ylabel("Cantidad")
        fig = ax.get_figure()
        fig.tight_layout()
        fig.savefig(gdir / f"{base}_status.png", dpi=150)
        fig.clf()


def load_from_csv(csv_path: Path) -> list[dict[str, object]]:
    d: list[dict[str, object]] = []
    if not csv_path.exists():
        return d
    with csv_path.open(encoding="utf-8") as h:
        r = csv.DictReader(h)
        for row in r:
            qid = row.get("id") or row.get("_id")
            if not qid:
                continue
            d.append({
                "_id": qid,
                "question_title": row.get("question_title", ""),
                "question_content": row.get("question_content", ""),
                "best_answer": row.get("best_answer", ""),
            })
    return d


def load_from_mongo(uri: str, db: str, coll: str) -> list[dict[str, object]]:
    client = connect_to_mongo(uri)
    c = client[db][coll]
    return list(c.find({}, {"question_title": 1, "question_content": 1, "best_answer": 1}))


def produce_stream(
    dis: str,
    prm: dict[str, float],
    total: int,
    uri: str,
    db: str,
    coll: str,
    csv_path: Path | None,
    out: Path,
    topic: str,
    parts: int,
    repl: int,
) -> None:
    q = load_from_csv(csv_path) if csv_path else []
    if not q:
        q = load_from_mongo(uri, db, coll)
    if not q:
        raise RuntimeError("No hay preguntas para publicar")
    prod = build_producer()
    ensure_topics([NewTopic(name=topic, num_partitions=parts, replication_factor=repl)])
    logf = mk_logname(dis, total, prm, out)
    rows: list[list[object]] = []
    g_rows: list[dict[str, object]] = []

    for _ in range(1, total + 1):
        wait = p_inter(prm["lmbda"]) if dis == "poisson" else u_inter(prm["low"], prm["high"])
        time.sleep(wait)
        doc = random.choice(q)
        m = QuestionMessage(
            question_id=str(doc.get("_id")),
            title=doc.get("question_title", ""),
            content=doc.get("question_content", ""),
            best_answer=doc.get("best_answer", ""),
        )
        prod.send(topic, key=m.message_id, value=m.asdict())
        prod.flush()
        ts = datetime.utcnow().isoformat()
        rows.append([ts, "PUB", m.message_id, m.question_id, "OK", wait, topic])
        g_rows.append({"ts": ts, "st": "OK"})

    log_csv(logf, rows)
    mk_graphs(logf, out, g_rows)
    print(f"Publicados {len(rows)} mensajes en {topic}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--total", type=int, default=100)
    parser.add_argument("--distribution", choices=["poisson", "uniform"], default="poisson")
    parser.add_argument("--lambda", dest="lmbda", type=float, default=1.5)
    parser.add_argument("--low", type=float, default=0.1)
    parser.add_argument("--high", type=float, default=0.5)
    parser.add_argument("--mongo-uri", default="mongodb://mongo:27017/")
    parser.add_argument("--mongo-db", default="yahoo_db")
    parser.add_argument("--mongo-coll", default="preguntas")
    parser.add_argument("--dataset-csv", type=Path)
    parser.add_argument("--output", type=Path, default=Path("/data/traffic"))
    parser.add_argument("--topic", default="questions_in")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=1)
    args = parser.parse_args()

    configure_logging()

    params = {"lmbda": args.lmbda, "low": args.low, "high": args.high}

    produce_stream(
        dis=args.distribution,
        prm=params,
        total=args.total,
        uri=args.mongo_uri,
        db=args.mongo_db,
        coll=args.mongo_coll,
        csv_path=args.dataset_csv,
        out=args.output,
        topic=args.topic,
        parts=args.partitions,
        repl=args.replication_factor,
    )
