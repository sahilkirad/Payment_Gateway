# Edge Gateway Agent

## 📌 Purpose
The **Edge Gateway Agent** is the entry point of the transaction pipeline.  
It takes a raw transaction from the database or incoming API, validates/normalizes it, and enriches it with additional metadata (timestamps, fraud score, intent, etc.).

## 🔹 Input
- A **transaction dictionary** (from DB or API):
  ```json
  {
    "txn_id": "TXN123",
    "amount": 5000,
    "account": "GB45UCQV...",
    "vendor_id": "VND1234",
    "intent": "Salary Disbursement",
    "sla_id": "SLA-LOW-958",
    ...
  }


## 🔹 Output
The same transaction dict augmented with extra fields like:

- **received_at** timestamp


  ```json
  {
  "txn_id": "TXN123",
  "amount": 5000,
  "account": "GB45UCQV...",
  "intent": "Salary Disbursement",
  "received_at": "2025-08-24T14:32:04.896785Z"
  }

## ⚙️ Notes

- This agent has no dependencies (it always runs first).

- Provides a clean, enriched transaction object to downstream agents.