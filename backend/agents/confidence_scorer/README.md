# Confidence Scorer Agent

## 📌 Purpose
The **Confidence Scorer Agent** estimates the reliability of routing options (banks or payment partners).  
It assigns **confidence scores** to candidate banks.

## 🔹 Input
- Output from `edge_gateway` (transaction details):
  ```json
  {
    "txn_id": "TXN123",
    "amount": 5000,
    "account": "GB45UCQV...",
    "intent": "Salary Disbursement",
    ...
  }


## 🔹 Output
A dict with:

- `status`: success/failure

- `txn_id`

- `confidence_scores`: dict of banks → score (0..1)


  ```json
  {
    "status": "success",
    "txn_id": "TXN123",
    "confidence_scores": {
      "Bank_A": 0.5,
      "Bank_B": 0.7,
      "Bank_C": 0.6
    }
  }


## ⚙️ Notes

- Depends on Edge Gateway.

- Uses heuristics for now (LLM disabled in free mode).

- Downstream Routing Planner consumes these scores.