import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "test")
KAFKA_SERVICE = os.environ.get("KAFKA_SERVICE")
