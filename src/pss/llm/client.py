import logging
import json
from typing import Any
from src.pss.llm.gemini_client import GeminiAPIClient

logger = logging.getLogger(__name__)

class LLMClient:
    def __init__(self):
        self.client = GeminiAPIClient()

    async def get_json_completion(self, prompt: str, system: str = "") -> Any:
        full_system = (
            f"{system}\n\n"
            "CRITICAL: Your response must be valid JSON only. No preamble, no markdown fences."
        )
        try:
            return await self.client.get_json_completion(prompt=prompt, system=full_system)
        except Exception as e:
            logger.error(f"Unexpected error in LLMClient: {e}")
            raise
