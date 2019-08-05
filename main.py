import logging
import os
import sys
import time

from kafka import KafkaConsumer
from kafka.conn import BrokerConnection
from kafka.errors import KafkaConnectionError, NoBrokersAvailable


def broker_connection():
    while True:
        print("======================")
        time.sleep(3)
        # servers = ["192.168.1.139:9094", "192.168.1.140:9094", "192.168.4.255:9094"]
        servers = ["192.168.43.127:9094"]

        for server in servers:
            host, port = server.split(':')
            print("Host + Port= ", host, port)
            try:
                broker_conn = BrokerConnection(host, port, 0)
                print("C = ", broker_conn.connected())
            except Exception as exc:
                print("Exception= ", str(exc))


# Start bar as a process
# p = multiprocessing.Process(target=broker_connection).start()

# Wait for 10 seconds or until process finishes
# p.join(10)

# If thread is still active
# if p.is_alive():
#     print("running... let's kill it...")
#
#     # Terminate
#     p.terminate()
#     p.join()

logging.basicConfig(level=logging.DEBUG)

# logging.basicConfig(
#     format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
#     level=logging.INFO
# )

# To consume latest messages and auto-commit offsets
try:
    consumer = KafkaConsumer(
        'versa-monitoring',
        group_id='parthi',
        # bootstrap_servers=["192.168.43.127:9094"],
        bootstrap_servers=["192.168.1.139:9094", "192.168.1.140:9094", "192.168.4.255:9094"],
        auto_offset_reset='latest',
    )
except (KafkaConnectionError, NoBrokersAvailable) as exc:
    print("-" * 150)
    print(exc)
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(exc_type, fname, exc_tb.tb_lineno)
    print("-" * 150)
    consumer = []
    raise Exception(str(exc))
except Exception as ex:
    print("=" * 150)
    print(ex)
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(exc_type, fname, exc_tb.tb_lineno)
    print("=" * 150)
    consumer = []


def consumer_subscribe_listner():
    print("consumer_subscribe_listner", "1" * 10000)


try:
    print("-" * 100)

    # print(type(consumer), "====",consumer, "====",consumer.__dict__)
    # while True:
    #     consumer.poll(2000)
    #     print(consumer._client._conns)
    #     if len(consumer._client._conns) > 0:
    #         node_id = list(consumer._client._conns.keys())[0]
    #         print(node_id, consumer._client._conns[node_id].state)
    #         if consumer._client._conns[node_id].state is ConnectionStates.DISCONNECTED:
    #             consumer.close()
    #         else:
    #             print("Connected")
    #     else:
    #         print("No Connection Found")

    # consumer.subscribe(['versa-monitoring'], listener=consumer_subscribe_listner() )
    print(consumer.subscription(), consumer.topics())
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("&" * 100)
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))
        print("&" * 100)

except Exception as err:
    print("=" * 150)
    print(err)
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(exc_type, fname, exc_tb.tb_lineno)
    print("=" * 150)

# consume earliest available messages, don't commit offsets
# KafkaConsumer(, enable_auto_commit=True)

# consume json messages
# KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))

# consume msgpack
# KafkaConsumer(value_deserializer=msgpack.unpackb)

# StopIteration if no message after 1sec
# KafkaConsumer()

# # Subscribe to a regex topic pattern
# consumer = KafkaConsumer()
# consumer.subscribe(pattern='^awesome.*')
#
# # Use multiple consumers in parallel w/ 0.9 kafka brokers
# # typically you would run each on a different server / process / CPU
# consumer1 = KafkaConsumer('my-topic',
#                           group_id='my-group',
#                           bootstrap_servers='my.server.com')
# consumer2 = KafkaConsumer('my-topic',
#                           group_id='my-group',
#                           bootstrap_servers='my.server.com')
