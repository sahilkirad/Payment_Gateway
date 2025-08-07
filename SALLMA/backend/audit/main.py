import ray
from explanation_agent import ExplanationAgent

def main():
    ray.init()

    # Create the agent
    agent = ExplanationAgent.remote()

    # Run the full pipeline (you can return results if needed)
    ray.get(agent.run.remote(bucket_name="sallma", folder_path="Audit/"))

    ray.shutdown()

if __name__ == "__main__":
    main()