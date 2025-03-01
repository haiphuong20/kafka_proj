import yaml
import os
from dotenv import load_dotenv

load_dotenv()

def load_config():
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)

    # Load sensitive data from environment variables
    config["kafka"]["username"] = os.getenv("KAFKA_USERNAME")
    config["kafka"]["password"] = os.getenv("KAFKA_PASSWORD")
    config["mongodb"]["uri"] = os.getenv("MONGO_URI")

    return config
