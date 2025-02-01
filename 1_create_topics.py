from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Визначення імені
my_name = "edwardprom1"

# Визначення топіків
topics_to_create = [
    NewTopic(name=f'{my_name}_building_sensors', num_partitions=2, replication_factor=1),
    NewTopic(name=f'{my_name}_temperature_alerts', num_partitions=2, replication_factor=1),
    NewTopic(name=f'{my_name}_humidity_alerts', num_partitions=2, replication_factor=1),
]

# Отримання існуючих топіків через KafkaConsumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

existing_topics = consumer.topics()
# print(f"Existing topics: {existing_topics}")

# Створення топіків, якщо їх ще не існує
for topic in topics_to_create:
    if topic.name in existing_topics:
        print(f"Topic '{topic.name}' already exists.")
    else:
        try:
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"Topic '{topic.name}' created successfully.")
        except Exception as e:
            print(f"An error occurred while creating topic '{topic.name}': {e}")

# Закриття зв'язку з клієнтом
admin_client.close()
consumer.close()
