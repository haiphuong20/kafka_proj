from pymongo import MongoClient

class MongoDB:
    def __init__(self, uri, database, collection):
        self.client = MongoClient(uri)
        self.collection = self.client[database][collection]

    def insert_bulk(self, data_list):
        if data_list:
            try:
                self.collection.insert_many(data_list, ordered=False)
                print(f"Inserted {len(data_list)} records into MongoDB.")
            except Exception as e:
                print(f"Error inserting into MongoDB: {e}")
