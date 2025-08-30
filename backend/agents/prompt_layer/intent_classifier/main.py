import ray
from .llm import classify_intent

ray.init(ignore_reinit_error=True)

@ray.remote
class IntentClassifierAgent:
    def run(self, query: str):
        return classify_intent(query)

# Example usage
if __name__ == "__main__":
    agent = IntentClassifierAgent.remote()
    result = ray.get(agent.run.remote("disburse 20k to vendor X from ICICI"))
    print(result)
