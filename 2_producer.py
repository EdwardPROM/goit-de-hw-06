# producer.py
from kafka import KafkaProducer
import json
import random
import time
import uuid
from configs import kafka_config

# Налаштування Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": kafka_config["bootstrap_servers"],
    "security_protocol": kafka_config["security_protocol"],
    "sasl_mechanism": kafka_config["sasl_mechanism"],
    "sasl_plain_username": kafka_config["username"],
    "sasl_plain_password": kafka_config["password"],
}

your_name = "edwardprom1"
TOPIC_SENSORS = f"{your_name}_building_sensors"


# Виробник (Producer) для відправки даних з датчиків у топік building_sensors
def produce_sensor_data():
    producer = KafkaProducer(
        **KAFKA_CONFIG, value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    sensor_id = random.randint(1, 5)
    for i in range(200):
        try:
            data = {
                "sensor_id": sensor_id,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "temperature": random.randint(25, 45),
                "humidity": random.randint(15, 85),
            }
            producer.send(
                TOPIC_SENSORS, key=str(uuid.uuid4()).encode("utf-8"), value=data
            )
            producer.flush()
            print(
                f"Повідомлення {i} надіслано в тему '{TOPIC_SENSORS}' успішно з даними: {data}"
            )
            time.sleep(2)
        except Exception as e:
            print(f"An error occurred: {e}")
    producer.close()


if __name__ == "__main__":
    produce_sensor_data()