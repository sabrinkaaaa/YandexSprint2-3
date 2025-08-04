from fastapi import FastAPI
import asyncio
from kafka_utils import produce_event, consume_events
from fastapi.responses import JSONResponse


app = FastAPI()

@app.on_event("startup")
async def startup():
    asyncio.create_task(consume_events())

@app.post("/api/events/{event_type}", status_code=201)
async def create_event(event_type: str):
    await produce_event(event_type)
    return JSONResponse(
        status_code=201,
        content={"status": "success", "type": event_type}
    )

@app.get("/api/events/health")
def health_check():
    return {"status": True}

