import logging
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import health, quote, options, holdings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Breeze Tiny Endpoint")


@app.on_event("startup")
def _log_proxy_config():
    """Log proxy environment at startup so Render logs show whether QuotaGuard is active."""
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "QUOTAGUARDSTATIC_URL"):
        val = os.environ.get(var)
        if val:
            # Mask password: http://user:PASS@host:port → http://user:***@host:port
            try:
                at = val.index("@")
                colon = val.rindex(":", 0, at)
                masked = val[:colon + 1] + "***" + val[at:]
            except ValueError:
                masked = val
            logger.info("PROXY CONFIG  %s = %s", var, masked)
        else:
            logger.warning("PROXY CONFIG  %s is NOT set", var)

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
app.include_router(holdings.router)
