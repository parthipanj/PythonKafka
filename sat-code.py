import codecs
import logging
import multiprocessing
import time

from kafka import KafkaConsumer


class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='192.168.4.255:9094,192.168.1.139:9094,192.168.1.140:9094',
                                 auto_offset_reset='latest',
                                 group_id='parthi-test')
        consumer.subscribe(['versa-monitoring'])

        while not self.stop_event.is_set():
            for message in consumer:
                log_msg = message.value if hasattr(message, 'value') else message

                decoded_value = codecs.escape_decode(log_msg)[0].decode('utf-8')
                decoded_value = replace_double_quotes(decoded_value)
                print(decoded_value)
                # if self.stop_event.is_set():
                #     break

        consumer.close()


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
    tasks = [
        Consumer()
    ]

    for t in tasks:
        print("start")
        t.start()

    time.sleep(10)

    for task in tasks:
        print("stop")
        task.stop()

    for task in tasks:
        print("join")
        task.join()


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
