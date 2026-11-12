import sys
import grpc
from concurrent import futures
from transformers import pipeline

sys.path.append('../proto')
import system_pb2
import system_pb2_grpc

class LLMServiceServicer(system_pb2_grpc.LLMServiceServicer):
    def __init__(self):
        print("Loading local LLM (TinyLlama)...")
        self.pipe = pipeline("text-generation", model="TinyLlama/TinyLlama-1.1B-Chat-v1.0", device=-1)
        print("LLM Loaded and ready.")

    def getLLMAnswer(self, request, context):
        print(f"Received query type: {request.context}")
        
        # Option B Specific LLM Integrations
        if request.context == "demand_prediction":
            prompt = f"<|system|>\nYou are an AI inventory analyst. Predict future demand based on the provided historical sales data.<|user|>\nHistorical data: {request.query}<|assistant|>\n"
        elif request.context == "reorder_suggestions":
            prompt = f"<|system|>\nYou are a supply chain assistant. Suggest which items to reorder based on the current stock levels.<|user|>\nCurrent stock: {request.query}<|assistant|>\n"
        elif request.context == "inventory_analytics":
            prompt = f"<|system|>\nYou are a warehouse manager. Provide a brief summarized report of the current inventory state.<|user|>\nInventory state: {request.query}<|assistant|>\n"
        else:
            prompt = f"<|system|>\nYou are an AI assistant.<|user|>\n{request.query}<|assistant|>\n"

        response = self.pipe(prompt, max_new_tokens=150, truncation=True)
        generated_text = response[0]['generated_text'].split("<|assistant|>\n")[-1].strip()
        
        return system_pb2.LLMAnswerResponse(
            request_id=request.request_id,
            answer=generated_text
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    system_pb2_grpc.add_LLMServiceServicer_to_server(LLMServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    print("LLM Server running on port 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()