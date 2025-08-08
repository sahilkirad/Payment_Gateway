import ray
import json
import asyncio
from backend.agents.finalizers.controller_agent import ControllerAgent
from backend.data_services.cockroach_client import db_client

async def main():
    # --- Setup ---
    print("--- Initializing Test Environment ---")
    ray.init(ignore_reinit_error=True)
    await db_client.connect()

    # --- Load Mock Data ---
    print("\n--- Loading Mock Data from tests/mock_data/controller_input.json ---")
    with open("tests/mock_data/controller_input.json", "r") as f:
        mock_data_list = json.load(f)
    
    # We will test with the first record from the file
    test_transaction = mock_data_list[0]
    print(f"Loaded transaction: {test_transaction.get('txn_id')}")

    # --- Deploy and Run the Agent ---
    print("\n--- Deploying and Running ControllerAgent ---")
    controller_actor = ControllerAgent.remote()
    
    # Call the agent's method and wait for the result
    final_state_ref = controller_actor.adjudicate_and_store.remote(test_transaction)
    final_state = await final_state_ref
    
    print("\n--- Agent Execution Complete ---")
    print("Final State Object returned by agent:")
    print(json.dumps(final_state, indent=2))

    # --- Shutdown ---
    print("\n--- Tearing Down Test Environment ---")
    await db_client.close()
    ray.shutdown()
    print("\n--- Test Complete ---")
    print("Check your CockroachDB UI at http://localhost:8081 to see the stored record.")

if __name__ == "__main__":
    asyncio.run(main())