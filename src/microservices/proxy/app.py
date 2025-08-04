import os
import random
import logging
import httpx
from fastapi import FastAPI, Request, Response

app = FastAPI()
logging.basicConfig(level=logging.INFO)

MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", 0))

@app.get("/api/movies")
async def proxy_movies(request: Request):
    if GRADUAL_MIGRATION:
        route_to_new = random.randint(1, 100) <= MIGRATION_PERCENT
    else:
        route_to_new = False

    target = MOVIES_SERVICE_URL if route_to_new else MONOLITH_URL
    logging.info(f"Routing to: {'MOVIES_SERVICE' if route_to_new else 'MONOLITH'}")

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{target}/api/movies")
            return Response(
                content=response.content,
                status_code=response.status_code,
                media_type="application/json"
            )
    except httpx.RequestError as e:
        logging.error(f"Request to {target} failed: {e}")
        return Response(content="Upstream error", status_code=502)
