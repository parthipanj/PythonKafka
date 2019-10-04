import codecs
import json
import logging
import settings
from kafka import KafkaConsumer
from kafka.errors import IllegalStateError


class Consumer:
    def __init__(self):
        self.topic = settings.KAFKA_TOPIC
        self.kafka_brokers = settings.KAFKA_BROKERS

    def consume(self):
        """
        Consume the messages from topic
        :return:
        """
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.kafka_brokers,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id='local'
            )
        except (IllegalStateError, AssertionError, TypeError, Exception) as error:
            logging.error('KAFKA_CONSUMER_ERROR: %s' % (str(error)))
        else:
            self.process_message(consumer)

    @staticmethod
    def process_message(consumer):
        """
        Process the consumed message
        :param consumer:
        :return:
        """
        for message in consumer:
            logging.info("message=%s topic=%s partition=%d" % (
                message.value,
                message.topic,
                message.partition
            ))
