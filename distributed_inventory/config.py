"""Shared defaults for the inventory cluster, LLM service and clients."""

DEFAULT_CLUSTER = ("localhost:50052", "localhost:50053", "localhost:50054")
DEFAULT_LLM_ADDRESS = "localhost:50051"
DEFAULT_LLM_PORT = 50051
DEFAULT_MODEL = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"

# Sample starting stock used by every node. Copied per node, never shared.
DEFAULT_INVENTORY = {"ItemA": 100, "ItemB": 50}

INSIGHT_FEATURES = ("demand_prediction", "reorder_suggestions", "inventory_analytics")
