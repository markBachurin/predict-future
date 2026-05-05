import logging
import json
from typing import Any
from src.pss.llm.gemini_client import GeminiCLIClient
from src.pss.llm.holdings import BIT_CAPITAL_HOLDINGS

logger = logging.getLogger(__name__)


class LLMClient:
    def __init__(self):
        self.client = GeminiCLIClient()

    async def get_json_completion(
        self,
        prompt: str,
        system: str = "",
    ) -> dict[str, Any]:
        full_system = (
            f"{system}\n\n"
            "CRITICAL: Your response must be valid JSON only — either a single object or an array. No preamble, no markdown fences, no explanation."
        )

        try:
            return await self.client.get_json_completion(prompt=prompt, system=full_system)
        except json.JSONDecodeError as e:
            logger.error(f"LLM returned invalid JSON: {e}")
            raise RuntimeError(f"Failed to parse LLM response as JSON: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in LLMClient: {e}")
            raise

    def get_holdings_context(self) -> str:
        return json.dumps(BIT_CAPITAL_HOLDINGS, indent=2)
