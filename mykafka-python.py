import logging
import codecs
import json

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import IllegalStateError, KafkaTimeoutError
from multiprocessing import Process


class Producer:
    def __init__(self):
        self.topic = "new_topic"
        self.kafka_brokers = ["localhost:9092"]

    def produce_message(self):
        """
        Produce message to the topic
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
            print("!" * 100, "KAFKA_PRODUCER_ERROR: %s" % (str(error)))
        else:
            data = {'foo': 'bar'}
            producer.send(self.topic, data)

            for n in range(100):
                data = {
                    'number': n,
                    'data': "{}-test".format(n)
                }
                producer.send(self.topic, data)
            else:
                producer.flush()
                producer.close()


class Consumer:
    def __init__(self):
        self.topic = "new_topic"
        self.kafka_brokers = ["localhost:9092"]

    def consume_message(self):
        """
        Consume the messages from topic
        :return:
        """
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.kafka_brokers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                # value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                # group_id='alarm'
            )
        except (IllegalStateError, AssertionError, TypeError, Exception) as error:
            print('*' * 100, 'KAFKA_CONSUMER_ERROR: %s' % (str(error)))
        else:
            for message in consumer:
                log_msg = message.value if hasattr(message, 'value') else message
                decoded_value = codecs.escape_decode(log_msg)[0].decode('utf-8')
                decoded_value = self.replace_double_quotes(decoded_value)
                print(message, decoded_value)

    @staticmethod
    def replace_double_quotes(data):
        """
        strip double quotes json format
        :param data:
        :return:
        """
        data = data.replace('{"{', "{'{")
        data = data.replace('}"}', "}'}")
        return data


def main():
    consumer_process = Process(target=Consumer().consume_message, args=())
    consumer_process.start()

    producer_process = Process(target=Producer().produce_message, args=())
    producer_process.start()


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
