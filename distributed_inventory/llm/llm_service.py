"""TinyLlama-backed gRPC service that turns inventory data into text insights.

The model runs on CPU. transformers and torch are imported only when the
service is constructed, so client and server code can be used without them.
"""

from concurrent import futures

import grpc

from ..config import DEFAULT_LLM_PORT, DEFAULT_MODEL
from ..proto import system_pb2, system_pb2_grpc


def build_prompt(context, query):
    if context == "demand_prediction":
        return f"<|system|>\nYou are an AI inventory analyst. Predict future demand based on the provided historical sales data.<|user|>\nHistorical data: {query}<|assistant|>\n"
    if context == "reorder_suggestions":
        return f"<|system|>\nYou are a supply chain assistant. Suggest which items to reorder based on the current stock levels.<|user|>\nCurrent stock: {query}<|assistant|>\n"
    if context == "inventory_analytics":
        return f"<|system|>\nYou are a warehouse manager. Provide a brief summarized report of the current inventory state.<|user|>\nInventory state: {query}<|assistant|>\n"
    return f"<|system|>\nYou are an AI assistant.<|user|>\n{query}<|assistant|>\n"


class LLMServiceServicer(system_pb2_grpc.LLMServiceServicer):
    def __init__(self, model_name=DEFAULT_MODEL):
        from transformers import pipeline

        print(f"Loading local LLM ({model_name})...")
        self.pipe = pipeline("text-generation", model=model_name, device=-1)
        print("LLM Loaded and ready.")

    def getLLMAnswer(self, request, context):
        print(f"Received query type: {request.context}")
        prompt = build_prompt(request.context, request.query)

        response = self.pipe(prompt, max_new_tokens=150, truncation=True)
        generated_text = response[0]['generated_text'].split("<|assistant|>\n")[-1].strip()

        return system_pb2.LLMAnswerResponse(
            request_id=request.request_id,
            answer=generated_text
        )


def serve(port=DEFAULT_LLM_PORT, model_name=DEFAULT_MODEL, max_workers=5, block=True):
    """Start the LLM service on [::]:port. With block=False, returns the started server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    system_pb2_grpc.add_LLMServiceServicer_to_server(LLMServiceServicer(model_name), server)
    server.add_insecure_port(f"[::]:{port}")
    print(f"LLM Server running on port {port}...")
    server.start()
    if block:
        server.wait_for_termination()
    return server
