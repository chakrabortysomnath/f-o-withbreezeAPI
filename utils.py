from fastapi import HTTPException, Request, Response


def require_right(user_right: str) -> str:
    """Normalize and validate 'call'/'put' input."""
    r = (user_right or "").strip().lower()
    if r not in ("call", "put"):
        raise HTTPException(status_code=400, detail="right must be 'call' or 'put'")
    return r


def safe_float(v):
    """Convert value to float safely, returning None on failure."""
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def nearest_strike_index(sorted_strikes: list[float], spot: float):
    """Return the index of the strike price nearest to spot."""
    if not sorted_strikes or spot is None:
        return None
    best_i = 0
    best_diff = abs(sorted_strikes[0] - spot)
    for i in range(1, len(sorted_strikes)):
        d = abs(sorted_strikes[i] - spot)
        if d < best_diff:
            best_diff = d
            best_i = i
    return best_i


def cors_preflight_response(request: Request) -> Response:
    """Return a standard CORS preflight 204 response."""
    return Response(
        status_code=204,
        headers={
            "Access-Control-Allow-Origin": request.headers.get("origin", "*"),
            "Access-Control-Allow-Methods": "POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,X-APP-TOKEN",
            "Access-Control-Max-Age": "86400",
        },
    )
