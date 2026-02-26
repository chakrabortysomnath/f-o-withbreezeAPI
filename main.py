from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import health, quote, options

app = FastAPI(title="Breeze Tiny Endpoint")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(quote.router)
app.include_router(options.router)
