import json
import settings
import logging
import time
import random
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError


class Producer:
    def __init__(self):
        self.topic = settings.KAFKA_TOPIC
        self.kafka_brokers = settings.KAFKA_BROKERS

    def produce(self):
        """
        Initialize kafka Producer
        :return:
        """
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_brokers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all',
                max_in_flight_requests_per_connection=1,
                retries=10
            )
        except (KafkaTimeoutError, Exception) as error:
            logging.error("KAFKA_PRODUCER_ERROR: %s" % (str(error)))
            raise Exception(error)
        else:
            self.send(producer)

    def send(self, producer):
        """
        Send message to the configured topic
        :param producer:
        :return:
        """
        try:
            while True:
                time.sleep(5)
                ts = time.time()
                temperature_data = {
                    'date': str(ts),
                    'data': {
                        'temp': round(random.uniform(-10, 50), 2)
                    }
                }

                producer.send(self.topic, temperature_data)

                logging.info("Kafka Produced Message {}".format(temperature_data))

        except Exception as exc:
            logging.error("KAFKA_PRODUCER_ERROR %s" % str(exc))
            self.close(producer)

    @staticmethod
    def close(producer):
        """
        Close the Kafka Producer
        :param producer:
        :return:
        """
        producer.flush()
        producer.close()
