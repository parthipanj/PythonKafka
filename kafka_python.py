#!/usr/bin/env python
import multiprocessing
import threading
import time

from kafka import KafkaConsumer, KafkaProducer


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers=["192.168.1.139:9094", "192.168.1.140:9094", "192.168.4.255:9094"])

        while not self.stop_event.is_set():
            producer.send('versa-monitoring', b"test")
            producer.send('versa-monitoring', b"\xc2Hola, mundo!")
            time.sleep(1)

        producer.close()


class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(
            bootstrap_servers=["192.168.1.139:9094", "192.168.1.140:9094", "192.168.4.255:9094"],
            auto_offset_reset='latest',
            group_id='alarm',
            consumer_timeout_ms=1000
        )
        consumer.subscribe(['versa-monitoring'])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                # if self.stop_event.is_set():
                #     break

        consumer.close()


def main():
    # tasks = [
    #     # Producer(),
    #     Consumer()
    # ]
    # print(tasks)
    #
    # for t in tasks:
    #     t.start()
    #
    # time.sleep(10)
    #
    # for task in tasks:
    #     task.stop()
    #
    # for task in tasks:
    #     task.join()

    consumer = Consumer()
    print(type(consumer))
    consumer.start()
    time.sleep(10)
    consumer.stop()
    consumer.join()


if __name__ == "__main__":
    # logging.basicConfig(
    #     format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    #     level=logging.INFO
    # )
    main()
