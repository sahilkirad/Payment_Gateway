import ray
from ray import serve
from fastapi import FastAPI
import pandas as pd
import joblib
from typing import List

app = FastAPI()

@serve.deployment(
    name="RouteTypePredictor",
    num_replicas=2,
    ray_actor_options={"num_cpus": 0.5}
)
@serve.ingress(app)
class ModelServer:
    def __init__(self):
        model_path = 'ml/models/route_type_predictor_v1.pkl'
        encoder_path = 'ml/models/route_type_label_encoder.pkl' # Path to the new file
        try:
            self.model = joblib.load(model_path)
            # Load the label encoder
            self.label_encoder = joblib.load(encoder_path)
            print(f"✅ Model and Label Encoder loaded successfully.")
        except FileNotFoundError as e:
            print(f"❌ CRITICAL ERROR: Could not load model or encoder: {e}")
            self.model = None
            self.label_encoder = None

    @app.post("/predict")
    def predict(self, transaction_features: List[dict]) -> dict:
        if not self.model or not self.label_encoder:
            return {"error": "Model or encoder is not loaded."}
        try:
            input_df = pd.DataFrame(transaction_features)
            # Model predicts the encoded numbers (e.g., [0, 2])
            encoded_predictions = self.model.predict(input_df)
            # Use the encoder to transform numbers back to text labels
            decoded_predictions = self.label_encoder.inverse_transform(encoded_predictions)
            
            return {"predictions": decoded_predictions.tolist()}
        except Exception as e:
            return {"error": str(e)}

model_server_app = ModelServer.bind()