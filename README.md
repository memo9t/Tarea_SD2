# Sistema Distribuido de Procesamiento de Preguntas y Respuestas

Este proyecto implementa un **sistema distribuido para procesar preguntas y respuestas** basado en un dataset real de Yahoo Answers.  
El sistema incluye generación de tráfico, gestión de caché con políticas configurables, simulación de un modelo LLM, evaluación de similitud mediante Apache Flink y almacenamiento de resultados en MongoDB.

# Arquitectura del sistema

El sistema está compuesto por 6 servicios en Docker:

- Traffic Producer = genera preguntas desde la base de datos inicial (bdd.json) y las publica en el tópico questions_in de Kafka.

- Buffer Service = ractúa como caché distribuido (políticas LRU, LFU o FIFO). Si la pregunta no está en caché o MongoDB, la envía al modelo LLM simulado.

- Text AI Consumer = simula un modelo de lenguaje (LLM) que genera respuestas automáticas y las envía al tópico llm_responses.

- Flink Job (Score Service) = compara la respuesta del modelo con la respuesta del dataset usando TF-IDF y similitud de coseno, generando un score de calidad.

- Repository Service (Storage) = guarda los resultados procesados y métricas de rendimiento en MongoDB.

- MongoDB = almacena las colecciones questions, results y metrics, con información sobre las preguntas, respuestas, hits/misses y puntajes.


# Requisitos previos

- Docker
- Docker Compose
- Python 3.10+

##  Flujo de datos

[MongoDB: questions]
       ↓
[Traffic Producer]
       ↓ Kafka (questions_in)
[Buffer Service]
       ↓ Kafka (llm_requests)
[Text AI Consumer]
       ↓ Kafka (llm_responses)
[Flink Job]
       ↓ Kafka (validated_responses)
[Repository Service]
       ↓
[MongoDB: results + metrics]


# Instalación y uso
- Clonar el repositorio
```bash
git clone <link del github>
cd <carpeta raiz del repositorio>
```


- Levantar el sistema
```bash
docker compose up -d
```
- Verificar que los contenedores estén corriendo

```bash
docker compose ps

```
# Monitoreo del sistema
- Logs del productor
```bash
docker compose logs -f traffic-producer
```
- Logs del buffer
```bash
docker compose logs -f buffer-service

```
- Logs del modelo (simulación LLM)
```bash
docker compose logs -f text-ai-consumer

```
- Logs del evaluador (Flink)
```bash
docker compose logs -f flink-submit

```
- Logs del almacenamiento
```bash
docker compose logs -f repository-service

```
# Revisa que Kafka tenga los topics creados
```bash
docker exec -it $(docker ps -qf name=kafka) kafka-topics --list --bootstrap-server localhost:9092

```

##  politicas y distribucines 
las politicas y distribuciones quedaron constantes en poisson y lru

# Verificación en MongoDB

Ingresar al contenedor de MongoDB:

```bash
docker exec -it sisdis-mongo-1 mongosh
```

Dentro del shell:

```bash
show dbs
use yahoo_db
show collections
db.answers.findOne()
```
Esto mostrará documentos con la pregunta, respuesta del dataset, respuesta del LLM, hits, misses y score.

# Apagar el sistema
```bash
docker compose down
```











