# routing_planner/route_selector.py

import json
from langgraph.graph import StateGraph, END
from .config import PlannerState
from .fallback_precompute import (
    generate_routes,
    llm_confidence_batch,
    llm_deliberate_weights,
)

# ------------------------
# Optimizer core
# ------------------------
def plan_selection(ctx, routes, weights):
    lat_vals = [r.latency_avg_ms for r in routes]
    fee_vals = [r.fee_bps for r in routes]
    min_lat, max_lat = min(lat_vals), max(lat_vals)
    min_fee, max_fee = min(fee_vals), max(fee_vals)

    def norm(v, lo, hi):
        return 0.0 if hi == lo else (v - lo) / (hi - lo)

    scored = []
    for r in routes:
        latency_norm = norm(r.latency_avg_ms, min_lat, max_lat)
        fee_norm = norm(r.fee_bps, min_fee, max_fee)
        util = (
            weights["confidence"] * r.confidence_score
            - weights["latency"] * latency_norm
            - weights["fee"] * fee_norm
            - weights["load"] * r.load_factor
        )
        scored.append((util, r.bank_id, latency_norm, fee_norm))

    ranked = sorted(scored, key=lambda x: x[0], reverse=True)
    primary = ranked[0][1]
    fallbacks = [b for _, b, _, _ in ranked[1:3]]

    prim = next(rt for rt in routes if rt.bank_id == primary)
    if prim.latency_avg_ms > ctx.sla_deadline_ms and len(ranked) > 1:
        alt = ranked[1][1]
        alt_rt = next(rt for rt in routes if rt.bank_id == alt)
        if alt_rt.latency_avg_ms < prim.latency_avg_ms and alt_rt.confidence_score >= prim.confidence_score * 0.95:
            primary, fallbacks = alt, [b for b in [prim.bank_id] + fallbacks if b != alt][:2]

    return {
        "primary_route": primary,
        "fallback_routes": fallbacks,
        "weights": weights,
        "ranking": [{"bank_id": b, "utility": round(u, 4)} for (u, b, _, _) in ranked],
    }


# ------------------------
# LangGraph orchestration
# ------------------------
def node_generate(state: PlannerState) -> PlannerState:
    routes = generate_routes(state.ctx.txn_id, k=5)
    state.routes = routes
    state.traces.append({"node": "generate_routes", "routes": [r.model_dump() for r in routes]})
    return state


def node_confidence(state: PlannerState) -> PlannerState:
    scores = llm_confidence_batch(state.routes)
    for r, (conf, why) in zip(state.routes, scores):
        r.confidence_score = conf
        r.rationale = why
    state.traces.append({"node": "confidence_scorer", "routes": [r.model_dump() for r in state.routes]})
    return state


def node_deliberate(state: PlannerState) -> PlannerState:
    w, why = llm_deliberate_weights(state.ctx, state.routes, state.recent_feedback)
    state.weights = w
    state.traces.append({"node": "deliberator", "weights": w, "why": why})
    return state


def node_optimize(state: PlannerState) -> PlannerState:
    selection = plan_selection(state.ctx, state.routes, state.weights)
    state.selection = selection
    state.traces.append({"node": "optimizer", "selection": selection})
    return state


def build_graph():
    g = StateGraph(PlannerState)
    g.add_node("generate", node_generate)
    g.add_node("confidence", node_confidence)
    g.add_node("deliberate", node_deliberate)
    g.add_node("optimize", node_optimize)

    g.set_entry_point("generate")
    g.add_edge("generate", "confidence")
    g.add_edge("confidence", "deliberate")
    g.add_edge("deliberate", "optimize")
    g.add_edge("optimize", END)
    return g.compile()
