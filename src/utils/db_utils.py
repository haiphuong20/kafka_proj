import json

from pymongo import MongoClient
from pymongo.write_concern import WriteConcern


class MongoDB:
    def __init__(self, uri, database, collection):
        self.client = MongoClient(
            uri,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000,
            socketTimeoutMS=20000,
        )

        self.collection = self.client[database].get_collection(
            collection,
            write_concern=WriteConcern(w="majority"),
        )

    def ping(self):
        self.client.admin.command("ping")

    def upsert_event(self, envelope, payload):
        source = envelope["source"]

        # Use a JSON array to avoid ambiguous IDs when fields contain separators.
        document_id = json.dumps(
            [
                source["id"],
                source["topic"],
                source["partition"],
                source["offset"],
            ],
            separators=(",", ":"),
            ensure_ascii=True,
        )

        document = {
            "_id": document_id,
            "source": source,
            "key_b64": envelope["key_b64"],
            "value_b64": envelope["value_b64"],
            "headers": envelope.get("headers", []),
            "payload": payload,
        }

        self.collection.replace_one(
            {"_id": document_id},
            document,
            upsert=True,
        )

        return document_id

    def close(self):
        self.client.close()