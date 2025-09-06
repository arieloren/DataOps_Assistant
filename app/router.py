import os
from typing import Optional, Dict, Any
from gemini import GeminiClient

class LLMRouter:
    def __init__(self, mode: str = "closed", closed_client: Optional[GeminiClient] = None):
        self.mode = mode
        if mode == "closed":
            api_key = os.getenv("GEMINI_API_KEY")
            if not api_key:
                raise ValueError("GEMINI_API_KEY environment variable required for closed mode")
            self.closed = closed_client or GeminiClient(api_key=api_key)
        else:
            raise ValueError(f"Phase A only supports mode='closed', got {mode}")

    def generate(self, prompt: str, **kwargs) -> str:
        assert self.mode == "closed", "Phase A uses closed LLM only"
        
        response = self.closed.generate(
            model=kwargs.get("model", "gemini-2.5-flash"),
            prompt=prompt,
            max_tokens=kwargs.get("max_tokens", 2000),
            temperature=kwargs.get("temperature", 0.1)
        )
        
        return response["content"]
