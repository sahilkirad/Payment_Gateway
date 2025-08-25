# Zone Classifier Agent

## 📌 Purpose
The **Zone Classifier Agent** determines the geographical / regulatory zone of the transaction, e.g. NATIONAL vs INTERNATIONAL.  
This classification helps downstream agents (like routing planner) apply different rules.

## 🔹 Input
- Output from `edge_gateway`:
  ```json
  {
    "txn_id": "TXN123",
    "amount": 5000,
    "account": "GB45UCQV...",
    "intent": "Salary Disbursement",
    ...
  }


## 🔹 Output
A dict containing:

- `status`: success/failure

- `txn_id`

- `zone`: "NATIONAL" or "INTERNATIONAL"/whatever


  ```json
  {
    "status": "success",
    "txn_id": "TXN123",
    "zone": "NATIONAL"
  }


## ⚙️ Notes

- Depends on Edge Gateway.

- Lightweight classification logic (rule-based for now, could be ML later).