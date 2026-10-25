# Kafka Product View Pipeline

A Python learning project that consumes product-view events from a source Kafka cluster, forwards them to a local Kafka cluster, and stores them in MongoDB.

```text
Source Kafka: product_view
          │
          │ bridge: deliver to local Kafka, then commit source offset
          ▼
Local Kafka: product_view.local
          │
          │ sink: upsert into MongoDB, then commit local offset
          ▼
MongoDB: kafka_project.product_views
```

## Project structure

```text
kafka_proj/
├── .env.example                  # Configuration template
├── .env                          # Runtime configuration (not tracked)
├── requirements.txt
└── src/
    ├── main.py                   # CLI entry point and shutdown handling
    ├── kafka_producer.py         # Source-to-local Kafka bridge
    ├── kafka_consumer.py         # Local Kafka-to-MongoDB sink
    └── utils/
        ├── __init__.py
        ├── config_loader.py     # Environment loading and validation
        ├── kafka_utils.py       # Kafka clients, polling, delivery confirmation
        └── db_utils.py          # MongoDB connection and idempotent upserts
```

## Prerequisites

- Python 3.10 or later with `venv` support.
- Docker and a running three-broker Kafka cluster from the coaching lab.
- Access to the source Kafka cluster and valid source credentials.
- MongoDB reachable through `MONGO_URI`.

Python runs on the host. The local broker endpoints below match the coaching Compose configuration: SASL_PLAINTEXT ports `9094`, `9194`, and `9294`. Kafka CLI commands run inside `kafka-0` using the internal PLAINTEXT listener on `29092`.

## 1. Install dependencies

Run from the project directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## 2. Configure the application

For initial setup, copy the template and fill in every required setting:

```bash
cp .env.example .env
nano .env
```

Do not copy the template over an existing configured `.env`. The application loads `.env` only; `.env.example` is a reference template, not an automatic fallback.

| Variable | Purpose / lab value |
| --- | --- |
| `SOURCE_BOOTSTRAP_SERVERS` | `source-kafka.example.com:9094` |
| `SOURCE_SECURITY_PROTOCOL` | `SASL_PLAINTEXT` |
| `SOURCE_SASL_MECHANISM` | `PLAIN` |
| `SOURCE_USERNAME`, `SOURCE_PASSWORD` | Credentials supplied by the coach |
| `SOURCE_TOPIC` | `product_view` |
| `SOURCE_GROUP_ID` | `phuong-product-view-bridge-v1` |
| `SOURCE_ID` | `coach-product-view-v1` |
| `LOCAL_BOOTSTRAP_SERVERS` | `localhost:9094,localhost:9194,localhost:9294` |
| `LOCAL_SECURITY_PROTOCOL` | `SASL_PLAINTEXT` |
| `LOCAL_SASL_MECHANISM` | `PLAIN` |
| `LOCAL_USERNAME`, `LOCAL_PASSWORD` | Kafka broker credentials from the local JAAS configuration; AKHQ login credentials are separate |
| `LOCAL_TOPIC` | `product_view.local` |
| `LOCAL_GROUP_ID` | `phuong-product-view-mongo-v1` |
| `MONGO_URI` | `mongodb://localhost:27017/` |
| `MONGO_DATABASE` | `kafka_project` |
| `MONGO_COLLECTION` | `product_views` |

The source hostname above is a placeholder. Set the actual private endpoint in your untracked `.env` file.

Both modes currently validate all required configuration fields. Existing shell environment variables take precedence over `.env`. Keep credentials out of Git; `.env` is ignored.

Keep `SOURCE_ID` stable when replaying the same source records. Review this identity if the source topic is deleted and recreated, since partition offsets can be reused. Group IDs control Kafka checkpoints and are separate from source identity.

## 3. Prepare local services

Check existing containers:

```bash
docker ps -a
```

If the existing Kafka containers are stopped:

```bash
docker start kafka-0 kafka-1 kafka-2
```

If the cluster has not been created, initialize it from the coaching repository:

```bash
cd ~/Desktop/de-coaching-lab/kafka/00-setup/kafka
docker network inspect streaming-network || docker network create streaming-network
docker compose up -d
```

Return to this project directory afterward. Once Kafka is ready, create and inspect the local topic:

```bash
docker exec kafka-0 kafka-topics \
  --bootstrap-server kafka-0:29092 \
  --create --if-not-exists \
  --topic product_view.local \
  --partitions 3 --replication-factor 3

docker exec kafka-0 kafka-topics \
  --bootstrap-server kafka-0:29092 \
  --describe --topic product_view.local
```

For a new local MongoDB instance, if port 27017 is available:

```bash
docker run -d \
  --name kafka-project-mongo \
  -p 127.0.0.1:27017:27017 \
  -v kafka-project-mongo-data:/data/db \
  mongo:7
```

If that container already exists, use `docker start kafka-project-mongo` instead. If using another MongoDB instance, configure its URI rather than creating another container.

```bash
docker exec kafka-project-mongo mongosh \
  --quiet --eval 'db.adminCommand({ping: 1})'
```

## 4. Run the pipeline

Use two terminals from the project directory, activating the virtual environment in each.

Terminal 1 — start the sink:

```bash
source .venv/bin/activate
python src/main.py sink
```

Terminal 2 — start the bridge:

```bash
source .venv/bin/activate
python src/main.py bridge
```

Keep both terminals open. Expected successful processing logs include:

```text
Delivered and committed: source=... local=...
Saved and committed: local=... mongo_id=...
```

A `Sink started` or `Bridge started` log alone does not prove Kafka authentication or record processing succeeded. Check the subsequent delivery/write logs. Press Ctrl+C in each terminal to request shutdown; an in-flight operation may take time to finish or time out.

## 5. Inspect data and progress

The following commands assume the default names from `.env.example`.

Count MongoDB documents:

```bash
docker exec kafka-project-mongo mongosh kafka_project \
  --quiet --eval 'db.product_views.countDocuments({})'
```

Inspect one document:

```bash
docker exec kafka-project-mongo mongosh kafka_project \
  --quiet --eval 'printjson(db.product_views.findOne())'
```

Browse documents interactively:

```bash
docker exec -it kafka-project-mongo mongosh kafka_project
```

Inside `mongosh`:

```javascript
db.product_views.find({}, { _id: 0, source: 1, "payload.data": 1 }).pretty()
it // Show the next page.
exit
```

Alternatively, connect MongoDB Compass to `mongodb://localhost:27017/`.

Inspect sink offsets and lag:

```bash
docker exec kafka-0 kafka-consumer-groups \
  --bootstrap-server kafka-0:29092 \
  --describe --group phuong-product-view-mongo-v1
```

AKHQ is available at `http://localhost:8180` when its container is running. Select the local connection and inspect the sink consumer group. Lag is based on committed offsets; it can fluctuate while new events arrive.

## Data format

The bridge preserves source key/value bytes as Base64 inside a versioned JSON envelope, with source ID, topic, partition, offset, timestamp metadata, and headers. The original key is also used as the local Kafka key. Source and local partition numbers need not match.

The sink decodes the source value into one of these representations:

- JSON: `payload.format = "json"`, parsed value in `payload.data`.
- UTF-8 text: `payload.format = "text"`.
- Other bytes: `payload.format = "base64"`.
- A null source value: `payload = null`.

Original Base64 data remains in the document. Base64 is an encoding, not encryption. A null value on the local topic itself is rejected because the sink expects an envelope; a null source value inside a valid envelope is supported. Source nulls are stored as events, not translated into MongoDB deletions.

MongoDB `_id` is a serialized array of `[source_id, source_topic, source_partition, source_offset]`. The sink uses `replace_one(..., upsert=True)` with this ID.

## Delivery guarantees and failure behavior

- Source offsets are committed synchronously only after local Kafka delivery is confirmed.
- Local offsets are committed synchronously only after MongoDB acknowledges the write.
- Auto-commit and automatic offset storage are disabled.
- Kafka producer idempotence is enabled, with `acks=all`.
- MongoDB writes use majority write concern. A standalone MongoDB server still has no replica redundancy.

The pipeline uses at-least-once processing. A crash after a successful delivery/write but before its offset commit can replay a record. MongoDB upserts prevent additional documents for the same source coordinates, including when the bridge forwards that record at a different local offset. This is not an end-to-end exactly-once transaction and does not deduplicate separate business events at different source offsets.

Records are processed sequentially for clarity. Connection, delivery, validation, write, and commit errors propagate and stop the process; there is no application retry loop, dead-letter queue, or automatic restart. Client libraries may perform their own retries. An invalid record can block progress until its cause is resolved. This implementation is intended for learning rather than high-throughput production deployment.

## 6. Verify restart and replay

### Resume

1. Stop the bridge so the local dataset stops growing.
2. Let the sink catch up; verify its lag is zero.
3. Stop the sink and record the MongoDB document count.
4. Restart the sink with the same group ID.
5. Verify it waits for new data rather than replaying committed records.

`auto.offset.reset=earliest` is used only when no valid committed offset exists. It does not override an existing checkpoint.

### Replay with a new sink group

Keep the bridge stopped and stop the original sink. Start a group ID that has never been used:

```bash
LOCAL_GROUP_ID=phuong-product-view-mongo-replay-v1 \
  python src/main.py sink
```

Inspect this replay group's lag:

```bash
docker exec kafka-0 kafka-consumer-groups \
  --bootstrap-server kafka-0:29092 \
  --describe --group phuong-product-view-mongo-replay-v1
```

When it reaches zero, compare the document count with the baseline. If all retained records were already stored, the count should remain unchanged. Use a different group suffix for another full replay. Replay reads only records still retained by Kafka.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Missing environment variable | Ensure the setting exists in `.env`, not only `.env.example`. |
| SASL authentication failure at the remote IP | Check `SOURCE_USERNAME` and `SOURCE_PASSWORD`. |
| SASL authentication failure at localhost | Check local broker JAAS credentials and the `LOCAL_` settings. |
| Credentials still appear wrong after editing `.env` | Shell variables take precedence; unset stale exported variables and restart the process. |
| Host client connects initially, then times out | Check broker advertised listeners and reachability of all advertised endpoints. |
| MongoDB connection error | Check `MONGO_URI`, container state, and port mapping. |
| No messages printed after restart | The group may already be caught up; inspect lag and source activity. |
| Envelope validation error | Ensure the local topic contains bridge envelopes rather than unrelated raw messages. |

## Submission evidence

Save screenshots or logs of source-to-local delivery, MongoDB writes and a sample document, local group offsets/lag, AKHQ, and document counts before/after replay. Record actual results and limitations. The instructions in this README are a verification procedure, not a claim that every scenario has been executed.
