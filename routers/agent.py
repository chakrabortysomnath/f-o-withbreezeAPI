from __future__ import annotations

import json
import logging
import os

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from auth import require_auth
from utils import cors_preflight_response

router = APIRouter()
logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

SYSTEM_PROMPT = """You are an expert NSE options strategist specialising in covered call writing for Indian equity markets.

Analyse the provided Nifty 50 covered call scan and give clear, actionable recommendations.

Evaluation criteria:
- Liquidity first: prefer strikes where bid > 0 and spread < 2% of premium
- Strike selection by risk tolerance:
    conservative  → 8–12% OTM (protect upside, lower yield)
    moderate      → 3–8% OTM (balanced yield vs call-away risk)
    aggressive    → 1–3% OTM (maximum premium, higher call-away risk)
- Prioritise stocks with steady, non-volatile price history for covered calls
- For accumulation advice: prefer stocks where cost to complete a lot is moderate
  and the resulting covered call yield would be attractive

Always mention:
- The specific action (e.g. "Write 2 lots of TCS 4200 Call expiring 27-Mar-2026")
- The net premium collected per lot and in total
- The breakeven and maximum profit at expiry
- The main risk (stock called away if it rallies through strike)

Return ONLY valid JSON — no markdown, no explanation outside the JSON — with this structure:
{
  "top_picks": [
    {
      "stock": "TICKER",
      "strike": 0,
      "lots": 1,
      "action": "Write N lot(s) of TICKER STRIKE Call expiring DATE",
      "premium_per_lot": 0,
      "total_premium": 0,
      "annualised_yield_pct": 0.0,
      "breakeven": 0,
      "max_profit_pct": 0.0,
      "rationale": "...",
      "risk_note": "...",
      "confidence": "high"
    }
  ],
  "accumulation_priorities": [
    {
      "stock": "TICKER",
      "shares_needed": 0,
      "approx_cost": 0,
      "rationale": "..."
    }
  ],
  "market_commentary": "...",
  "caution_flags": ["..."]
}"""


class AgentRequest(BaseModel):
    scan_results: list[dict]
    expiry_date: str
    risk_tolerance: str = "moderate"   # conservative | moderate | aggressive
    income_goal_pct: float = 1.0       # monthly income target as % of portfolio value


@router.options("/agent/covered-call-advice")
def options_agent(request: Request):
    return cors_preflight_response(request)


@router.post("/agent/covered-call-advice")
def covered_call_advice(
    req: AgentRequest,
    x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN"),
):
    """
    Pass scan results to Claude (claude-sonnet-4-6) for ranked covered call
    recommendations and accumulation priorities.
    """
    require_auth(x_app_token)

    if not ANTHROPIC_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="ANTHROPIC_API_KEY is not configured. Add it to Render environment variables.",
        )

    try:
        import anthropic
    except ImportError:
        raise HTTPException(status_code=500, detail="anthropic package not installed on server")

    # Build a compact payload — send top-3 opportunities per stock to stay within token limits
    writable = [r for r in req.scan_results if r.get("can_write_calls") and r.get("opportunities")]
    partial = [r for r in req.scan_results if not r.get("can_write_calls") and r.get("held_qty", 0) > 0]

    user_content = json.dumps(
        {
            "expiry_date": req.expiry_date,
            "risk_tolerance": req.risk_tolerance,
            "monthly_income_goal_pct": req.income_goal_pct,
            "stocks_with_full_lot": [
                {
                    "stock": r["stock_code"],
                    "lots_held": r["lots_held"],
                    "avg_cost": r["avg_cost"],
                    "spot": r["spot"],
                    "top_opportunities": r["opportunities"][:3],
                }
                for r in writable
            ],
            "partial_positions": [
                {
                    "stock": r["stock_code"],
                    "held_qty": r["held_qty"],
                    "lot_size": r["lot_size"],
                    "shares_needed": r["shares_short"],
                    "approx_accumulation_cost": (r.get("accumulation_advice") or {}).get("approx_cost"),
                    "avg_cost": r.get("avg_cost"),
                }
                for r in partial
            ],
        },
        indent=2,
    )

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    logger.info("AGENT  calling claude-sonnet-4-6  writable=%d  partial=%d", len(writable), len(partial))

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
    )

    raw = message.content[0].text.strip()

    # Strip markdown code fences if Claude wraps the JSON
    if "```" in raw:
        start = raw.find("{")
        end = raw.rfind("}") + 1
        raw = raw[start:end]

    try:
        advice = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("AGENT  non-JSON response: %s", raw[:300])
        advice = {"raw_response": raw}

    return {"status": "ok", "advice": advice}
