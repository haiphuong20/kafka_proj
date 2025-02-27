import threading
from producer import produce_to_local_kafka
from consumer import save_to_mongodb

if __name__ == "__main__":
    threading.Thread(target=produce_to_local_kafka).start()
    threading.Thread(target=save_to_mongodb).start()
