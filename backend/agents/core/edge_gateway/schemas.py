from typing import List, Optional, Literal
from pydantic import BaseModel, Field, condecimal, constr
from datetime import datetime

# Define constrained types (Pydantic-style)
TxnIdStr = constr(strip_whitespace=True, min_length=6)
IfscStr = constr(strip_whitespace=True, min_length=4)
AmountDecimal = condecimal(gt=0)

# Literal type for priority
Priority = Literal["HIGH", "NORMAL"]

class TransactionIn(BaseModel):
    txn_id: TxnIdStr
    merchant_id: str
    vendor_id: str
    account: str
    ifsc: IfscStr
    amount: AmountDecimal
    currency: str = "INR"
    timestamp: datetime
    priority: Priority = "NORMAL"
    geo_state: Optional[str] = None
    kyc_hash: Optional[str] = None
    sla_target_sec: int = 2_000

class BatchIn(BaseModel):
    items: List[TransactionIn] = Field(..., min_items=1, max_items=10000)

class IngestResponse(BaseModel):
    request_id: str
    received: int
    accepted: int
    rejected: int