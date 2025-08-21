# routing_planner/fallback_precompute.py

import json, random
from typing import List, Tuple, Dict, Any

from .config import BANKS, Route, TxnContext, _USE_LLM

if _USE_LLM:
    from langchain_openai import ChatOpenAI
else:
    ChatOpenAI = None  # type: ignore


# ------------------------
# Synthetic data generator
# ------------------------
def generate_routes(txn_id: str, k: int = 5) -> List[Route]:
    banks = random.sample(BANKS, k=min(k, len(BANKS)))
    routes: List[Route] = []
    for b in banks:
        routes.append(
            Route(
                txn_id=txn_id,
                bank_id=b,
                latency_avg_ms=random.randint(50, 450),
                success_rate=round(random.uniform(0.86, 0.995), 3),
                failure_rate=0.0,
                fee_bps=random.randint(5, 35),
                load_factor=round(random.uniform(0.2, 0.95), 2),
            )
        )
    for r in routes:
        r.failure_rate = round(
            max(0.0, min(0.2, 1.0 - r.success_rate + random.uniform(-0.01, 0.01))),
            3,
        )
    return routes


# ------------------------
# Confidence scoring
# ------------------------
def rule_confidence(route: Route) -> float:
    score = (
        route.success_rate
        - 0.5 * route.failure_rate
        - 0.0007 * route.latency_avg_ms
        - 0.0008 * route.fee_bps
        - 0.15 * route.load_factor
    )
    return max(0.0, min(1.0, round(score, 3)))


def llm_confidence_batch(routes: List[Route]) -> List[Tuple[float, str]]:
    if not _USE_LLM:
        return [(rule_confidence(r), "rule") for r in routes]

    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.2)
    routes_dict = [r.model_dump() for r in routes]
    prompt = (
        "You are a routing reliability scorer. For each route JSON, output JSON array:\n"
        '{"bank_id": "...", "confidence_score": 0..1, "rationale": "..."}\n'
        f"ROUTES:\n{json.dumps(routes_dict)}"
    )
    msg = [{"role": "system", "content": prompt}]
    try:
        out = llm.invoke(msg).content.strip()
        parsed = json.loads(out)
        by_bank = {d["bank_id"]: d for d in parsed}
        result = []
        for r in routes:
            item = by_bank.get(r.bank_id, {})
            result.append(
                (
                    float(item.get("confidence_score", rule_confidence(r))),
                    item.get("rationale", "llm"),
                )
            )
        return result
    except Exception as e:
        return [(rule_confidence(r), f"llm_error:{e}") for r in routes]


# ------------------------
# Weight deliberation
# ------------------------
def default_weights(ctx: TxnContext) -> Dict[str, float]:
    w_conf = 0.55 + 0.2 * (1 - ctx.risk_appetite)
    w_lat = 0.25 + 0.2 * ctx.latency_sensitivity
    w_fee = 0.15 + 0.2 * ctx.fee_sensitivity
    w_load = 0.15
    s = w_conf + w_lat + w_fee + w_load
    return {k: v / s for k, v in dict(confidence=w_conf, latency=w_lat, fee=w_fee, load=w_load).items()}


def adapt_with_feedback(weights: Dict[str, float], feedback: List[Dict[str, Any]]) -> Dict[str, float]:
    w = dict(weights)
    if not feedback:
        return w
    recent = feedback[-5:]
    if any(f.get("sla_breached") for f in recent):
        w["latency"] *= 1.15
    if any(f.get("final_status") == "FAILED" for f in recent):
        w["confidence"] *= 1.1
    if any(f.get("cost_over_budget") for f in recent):
        w["fee"] *= 1.1
    s = sum(w.values())
    return {k: v / s for k, v in w.items()}


def llm_deliberate_weights(ctx: TxnContext, routes: List[Route], feedback: List[Dict[str, Any]]) -> Tuple[Dict[str, float], str]:
    if not _USE_LLM:
        w = adapt_with_feedback(default_weights(ctx), feedback)
        return w, "Default heuristic weights (no LLM)."

    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.2)
    prompt = (
        "You are a routing planner. Decide weights for:\n"
        "utility = w_conf*confidence - w_lat*latency_norm - w_fee*fee_norm - w_load*load_factor\n"
        "Rules:\n- Increase w_lat if SLA_deadline is tight\n- Increase w_conf if risk_appetite is low\n"
        "- Increase w_fee if fee_sensitivity is high\n"
        "Return JSON: {\"weights\": {...}, \"rationale\":\"...\"}\n"
        f"CTX: {ctx.model_dump()}\nFeedback: {feedback[-5:]}\n"
    )
    try:
        out = llm.invoke([{"role": "system", "content": prompt}]).content.strip()
        parsed = json.loads(out)
        w = parsed.get("weights", {})
        if not all(k in w for k in ("confidence", "latency", "fee", "load")):
            raise ValueError("weights keys missing")
        s = sum(w.values()) or 1.0
        w = {k: float(v) / s for k, v in w.items()}
        w = adapt_with_feedback(w, feedback)
        why = parsed.get("rationale", "LLM deliberation")
        return w, why
    except Exception as e:
        w = adapt_with_feedback(default_weights(ctx), feedback)
        return w, f"LLM error -> heuristic weights used: {e}"
