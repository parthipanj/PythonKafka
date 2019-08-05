import logging

from kafka import KafkaConsumer
from kafka.cluster import ClusterMetadata

logging.basicConfig(level=logging.DEBUG)


def check_broker_connection():
    print("C" * 100)


def cluster_connection():
    try:
        consumer = KafkaConsumer(
            'versa-monitoring',
            group_id='alarm',
            # bootstrap_servers=["192.168.43.127:9094"],
            bootstrap_servers=["192.168.1.139:9094", "192.168.1.140:9094", "192.168.4.255:9094"],
            auto_offset_reset='latest'
        )

        print("@" * 100)
        print(consumer.topics(), consumer.subscription(), consumer.assignment(), consumer.commit_async().__dict__)
        print("@" * 100)

        cluster = ClusterMetadata(
            retry_backoff_ms=1000,
            metadata_max_age_ms=3000,
            bootstrap_servers=["192.168.1.139:9094", "192.168.1.140:9094", "192.168.4.255:9094"]
            # bootstrap_servers=["192.168.43.127:9094"]
        )
    except Exception as error:
        print("S" * 100, str(error))
    else:
        print("-" * 100)
        print(cluster.brokers(), cluster.coordinator_for_group("alarm"))
        # print(cluster.request_update())
        # cluster.add_listener(check_broker_connection())


cluster_connection()
