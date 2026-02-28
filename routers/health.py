import logging
import os
import requests
from fastapi import APIRouter, Header
from auth import require_auth

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/health")
def health():
    """Basic liveness probe used by Render health-check. Always 200 while the process is alive."""
    return {"ok": True}


@router.get("/version")
def version():
    return {"version": "cors-options-quote-v1"}


@router.post("/echo")
def echo(payload: dict, x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")):
    require_auth(x_app_token)
    return {"status": "ok", "received": payload}


@router.get("/egress_ip")
def egress_ip(x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")):
    require_auth(x_app_token)
    r = requests.get("https://api.ipify.org?format=json", timeout=10)
    return r.json()


@router.get("/health/detailed")
def health_detailed(x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")):
    """
    Multi-layer health check (requires X-APP-TOKEN).

    Returns per-layer status for:
      backend   – FastAPI process (always ok if this endpoint responds)
      static_ip – outbound egress IP resolved via ipify
    """
    require_auth(x_app_token)

    layers: dict = {}

    # ── 1. Backend ────────────────────────────────────────────────────────────
    layers["backend"] = {
        "ok": True,
        "detail": "FastAPI process is running",
    }

    # ── 2. Static / egress IP ─────────────────────────────────────────────────
    proxy_url = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or os.environ.get("QUOTAGUARDSTATIC_URL")
    logger.info("EGRESS_IP_CHECK  proxy_active=%s", bool(proxy_url))
    try:
        r = requests.get("https://api.ipify.org?format=json", timeout=10)
        r.raise_for_status()
        ip = r.json().get("ip", "unknown")
        logger.info("EGRESS_IP_CHECK  resolved_ip=%s", ip)
        layers["static_ip"] = {
            "ok": True,
            "ip": ip,
            "detail": "Egress IP resolved",
        }
    except Exception as exc:
        logger.error("EGRESS_IP_CHECK  failed: %s", exc)
        layers["static_ip"] = {
            "ok": False,
            "ip": None,
            "detail": str(exc),
        }

    overall_ok = all(v["ok"] for v in layers.values())
    return {"ok": overall_ok, "layers": layers}
