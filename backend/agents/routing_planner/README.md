# Routing Planner Agent

## 📌 Purpose
The **Routing Planner Agent** selects the best route (bank or payment channel) for a transaction.  
It balances between:
- **confidence score**
- **latency**
- **fees**
- **load factors**
- **zone constraints**

## 🔹 Input
- Full **transaction details** (from Edge Gateway + Zone Classifier).
- A list of feedback dicts (Optional).

  ```json
  routing_planner_run(transaction, [Feedbacks])


## 🔹 Output
A dict with:

- `selection`: Dictionary with suggested bank with fallbacks.

- `routes`: List of banks with their metadata.

- `traces`: Honestly, idk.


  ```json
  {
  "selection": {...},
  "routes": [...],
  "traces": [...]
  }


## ⚙️ Notes

- Depends on **Zone Classifier**.

- Uses heuristics for weights unless LLM is enabled.

- Returns structured JSON that can be directly persisted in DB.