from pydantic import BaseModel

class IntentRequest(BaseModel):
    query: str

class IntentResponse(BaseModel):
    intent: str
    confidence: float
