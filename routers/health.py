import requests
from fastapi import APIRouter, Header
from auth import require_auth

router = APIRouter()


@router.get("/health")
def health():
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
