#!/bin/bash
set -e

echo "[init-mongo] Esperando que MongoDB esté disponible..."
until mongosh --host "$MONGO_INITDB_HOST" --port "$MONGO_INITDB_PORT" --eval "db.adminCommand({ ping: 1 })" >/dev/null 2>&1; do
    echo "[init-mongo] MongoDB aún no responde..."
    sleep 2
done

echo "[init-mongo] MongoDB disponible. Verificando contenido de la colección '$MONGO_INITDB_COLLECTION'..."
COUNT=$(mongosh --quiet --host "$MONGO_INITDB_HOST" --port "$MONGO_INITDB_PORT" \
    --eval "db.getSiblingDB('$MONGO_INITDB_DATABASE').getCollection('$MONGO_INITDB_COLLECTION').countDocuments()")

if [ "$COUNT" -gt 0 ]; then
    echo "[init-mongo] La colección '$MONGO_INITDB_COLLECTION' ya tiene datos ($COUNT documentos). Omitiendo importación."
else
    echo "[init-mongo] Importando datos iniciales desde /bdd.json..."
    mongoimport \
        --host "$MONGO_INITDB_HOST" \
        --port "$MONGO_INITDB_PORT" \
        --db "$MONGO_INITDB_DATABASE" \
        --collection "$MONGO_INITDB_COLLECTION" \
        --file /bdd.json \
        --jsonArray \
        --drop
    echo "[init-mongo] Importación completada."
fi
