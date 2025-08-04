import asyncio
import logging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "events"

async def produce_event(event_type: str):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        await producer.send_and_wait(KAFKA_TOPIC, event_type.encode('utf-8'))
        logging.info(f"Produced: {event_type}")
    finally:
        await producer.stop()

async def consume_events():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="cinemaabyss-consumer"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            logging.info(f"Consumed: {msg.value.decode('utf-8')}")
    finally:
        await consumer.stop()
