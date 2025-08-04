from fastapi import FastAPI
import asyncio
from kafka_utils import produce_event, consume_events

app = FastAPI()

@app.on_event("startup")
async def startup():
    asyncio.create_task(consume_events())

@app.post("/api/events/{event_type}")
async def send_event(event_type: str):
    await produce_event(event_type)
    return {"status": "produced", "type": event_type}
