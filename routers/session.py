import os
import logging

from fastapi import APIRouter, Header, Request
from breeze_connect import BreezeConnect

from auth import require_auth
from models import SessionRequest
from utils import cors_preflight_response

router = APIRouter()
logger = logging.getLogger(__name__)

BREEZE_API_KEY = os.environ.get("BREEZE_API_KEY", "")
BREEZE_API_SECRET = os.environ.get("BREEZE_API_SECRET", "")


@router.options("/session")
def options_session(request: Request):
    return cors_preflight_response(request)


@router.post("/session")
def create_session(
    req: SessionRequest,
    x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN"),
):
    """
    Generate a Breeze API session from a user-supplied session token.

    The caller must obtain `session_token` by visiting the ICICIDirect login URL:
      https://api.icicidirect.com/apiuser/login?api_key=<BREEZE_API_KEY>
    After logging in, the token appears in the redirected URL as the `apisession` param.
    """
    require_auth(x_app_token)

    if not (BREEZE_API_KEY and BREEZE_API_SECRET):
        return {"status": "error", "detail": "BREEZE_API_KEY or BREEZE_API_SECRET not set on server"}

    breeze = BreezeConnect(api_key=BREEZE_API_KEY)
    result = breeze.generate_session(
        api_secret=BREEZE_API_SECRET,
        session_token=req.session_token.strip(),
    )

    logger.info("SESSION_GENERATE  result=%s", result)

    if result and result.get("Status") == 200:
        data = result.get("Success", {}) or {}
        return {
            "status": "ok",
            "customer_id": data.get("idirect_userid"),
            "session_token": req.session_token.strip(),
            "detail": "Session generated successfully",
        }

    return {
        "status": "error",
        "detail": "Failed to generate session",
        "raw": result,
    }
