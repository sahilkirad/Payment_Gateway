# Main Orchestration (FastAPI + Ray)

## 📌 Purpose
The **Main Orchestration Layer**:
1. Fetches transaction data from the DB.
2. Uses the **Planner Agent (routing_graph LLM)** to generate a DAG of required agents.
3. Executes the DAG with **Ray** (agents run in parallel where possible).
4. Returns both the **generated DAG** and **execution results**.


## 🔹 Input
- API request:
  ```http
  POST /process-transaction
  {
    "txn_id": "TXN123"
  }


## 🔹 Output
A dict with:

- The generated DAG (nodes + edges).

- Execution results from each agent.


  ```json
  {
    "generated_dag": {
      "nodes": ["edge_gateway", "zone_classifier", "confidence_scorer", "routing_planner"],
      "edges": [["edge_gateway","zone_classifier"], ["edge_gateway","confidence_scorer"],
                ["zone_classifier","routing_planner"], ["confidence_scorer","routing_planner"]]
    },
    "execution_results": {
      "edge_gateway": {...},
      "zone_classifier": {...},
      "confidence_scorer": {...},
      "routing_planner": {...}
    }
  }


## ⚙️ Notes

- Uses Ray for parallel execution.

- Currently executes sequential loop; can be migrated to Ray DAG later.

- Errors in one agent are captured and returned in the execution_results.