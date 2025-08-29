# routing_planner/config.py

import os
from typing import Dict, Any, List
from pydantic import BaseModel, Field

# Load OPENAI key
_USE_LLM = bool(os.getenv("OPENAI_API_KEY"))

# Bank IDs we support
BANKS = ["HDFC", "ICIC", "SBIN", "PUNB", "KKBK", "UTIB"]

# Transaction context: what matters for optimization
class TxnContext(BaseModel):
    txn_id: str
    zone: str = "WEST-1"
    sla_deadline_ms: int = 200
    fee_sensitivity: float = 0.5
    latency_sensitivity: float = 0.7
    risk_appetite: float = 0.5

# Candidate route: bank telemetry + computed score
class Route(BaseModel):
    txn_id: str
    bank_id: str
    latency_avg_ms: int
    success_rate: float
    failure_rate: float
    fee_bps: int
    load_factor: float
    confidence_score: float = 0.0
    rationale: str = ""

# Planner agent state for LangGraph
class PlannerState(BaseModel):
    ctx: TxnContext
    routes: List[Route] = Field(default_factory=list)
    weights: Dict[str, float] = Field(default_factory=dict)
    selection: Dict[str, Any] = Field(default_factory=dict)
    traces: List[Dict[str, Any]] = Field(default_factory=list)
    recent_feedback: List[Dict[str, Any]] = Field(default_factory=list)
