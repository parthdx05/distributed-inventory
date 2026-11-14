"""LLM insight service. Run with: python -m distributed_inventory.llm --port 50051"""

from .llm_service import LLMServiceServicer, build_prompt, serve

__all__ = ["LLMServiceServicer", "build_prompt", "serve"]
