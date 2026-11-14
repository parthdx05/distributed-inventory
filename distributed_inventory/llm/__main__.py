"""Start the LLM insight service.

    python -m distributed_inventory.llm --port 50051

The first run downloads the model from Hugging Face, which needs internet
access unless the model is already cached.
"""

import argparse

from ..config import DEFAULT_LLM_PORT, DEFAULT_MODEL
from .llm_service import serve


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run the TinyLlama insight service.")
    parser.add_argument("--port", type=int, default=DEFAULT_LLM_PORT)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    args = parser.parse_args(argv)
    serve(port=args.port, model_name=args.model)


if __name__ == "__main__":
    main()
