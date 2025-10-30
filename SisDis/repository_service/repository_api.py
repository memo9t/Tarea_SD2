from __future__ import annotations
import argparse
import logging
from pymongo import UpdateOne

from core_lib.message_models import EvaluatedResult
from core_lib.kafka_client import build_consumer
from core_lib import configure_logging
from core_lib.mongo_client import connect_to_mongo

LOG = logging.getLogger(__name__)


class RepositoryAPI:
    def __init__(
        self,
        *,
        mongo_uri: str,
        database: str,
        collection: str,
        metrics_collection: str,
        topic: str,
        group_id: str,
    ):
        client = connect_to_mongo(mongo_uri)
        self.store = client[database][collection]
        self.metrics = client[database][metrics_collection]
        self.consumer = build_consumer(topic, group_id=group_id)

    def save_answer(self, res: EvaluatedResult) -> None:
        data = {
            "id_pregunta": res.question_id,
            "pregunta": res.title,
            "contenido": res.content,
            "respuesta_dataset": res.best_answer,
            "respuesta_llm": res.llm_answer,
            "score": res.score,
            "aceptado": res.accepted,
            "ultima_actualizacion": res.decided_at,
        }
        query = {"id_pregunta": res.question_id}
        update = {
            "$set": data,
            "$inc": {"contador_consultas": 1},
            "$setOnInsert": {"creado_en": res.decided_at},
        }
        self.store.update_one(query, update, upsert=True)

    def add_metrics(self, res: EvaluatedResult) -> None:
        update = UpdateOne(
            {"_id": "global"},
            {
                "$inc": {
                    "procesadas": 1,
                    "aceptadas": 1 if res.accepted else 0,
                    "rechazadas": 0 if res.accepted else 1,
                    "score_acum": float(res.score),
                },
                "$set": {
                    "ultimo_score": float(res.score),
                    "actualizado_en": res.decided_at,
                },
                "$setOnInsert": {"creado_en": res.decided_at},
            },
            upsert=True,
        )
        self.metrics.bulk_write([update])

    def run(self) -> None:
        LOG.info("RepositoryService activo y escuchando resultados validados")
        while True:
            records = self.consumer.poll(timeout_ms=1000)
            for _, msgs in records.items():
                for rec in msgs:
                    data = rec.value
                    res = EvaluatedResult(**data)
                    self.save_answer(res)
                    self.add_metrics(res)
                    LOG.info(
                        "Guardado correctamente",
                        extra={
                            "question_id": res.question_id,
                            "score": res.score,
                            "accepted": res.accepted,
                        },
                    )
                if msgs:
                    self.consumer.commit()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo-uri", default="mongodb://mongo:27017/")
    parser.add_argument("--db", default="yahoo_db")
    parser.add_argument("--collection", default="results")
    parser.add_argument("--metrics", default="metrics")
    parser.add_argument("--topic", default="validated_responses")
    parser.add_argument("--group-id", default="repository-service")
    args = parser.parse_args()

    configure_logging()

    service = RepositoryAPI(
        mongo_uri=args.mongo_uri,
        database=args.db,
        collection=args.collection,
        metrics_collection=args.metrics,
        topic=args.topic,
        group_id=args.group_id,
    )
    service.run()


if __name__ == "__main__":
    main()
