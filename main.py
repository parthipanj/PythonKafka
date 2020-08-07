import logging
import settings
from multiprocessing import Process
from stream.producer import Producer
from stream.consumer import Consumer


class Main:
    def __init__(self):
        self.service_type = settings.KAFKA_SERVICE

    def start(self):
        logging.info("Kafka Running Service: %s" % self.service_type)

        if self.service_type == "producer":
            producer_process = Process(target=Producer().produce, args=())
            producer_process.start()
        elif self.service_type == "consumer":
            consumer_process = Process(target=Consumer().consume, args=())
            consumer_process.start()


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    Main().start()
