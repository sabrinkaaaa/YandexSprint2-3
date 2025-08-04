from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import logging

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "events"

async def produce_event(event_type: str):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        await producer.send_and_wait(KAFKA_TOPIC, event_type.encode())
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
            logging.info(f"Consumed: {msg.value.decode()}")
    finally:
        await consumer.stop()
