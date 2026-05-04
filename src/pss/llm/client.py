import anthropic
import logging
import json
from typing import Any
from pss_config.config import settings
from src.pss.llm.holdings import BIT_CAPITAL_HOLDINGS

logger = logging.getLogger(__name__)


class LLMClient:
    def __init__(self, api_key: str = settings.anthropic_api_key):
        if not api_key:
            raise ValueError("Anthoropic api key must set in environment")

        self.client = anthropic.AsyncAnthropic(api_key=api_key)
        self.gatekeeper_model = "claude-3-haiku-20240307"
        self.reasoner_model = "claude-3.5-sonnet-20240628"

    async def get_json_completion(self, prompt: str, system: str = "", model: str | None = None, temperature : float = 0.0) -> dict[str, Any]:
        model_to_use = model or self.gatekeeper_model

        # ingest strict json instructions into system prompt
        full_system = {
            f"{system}\n\n"
            "CRITICAL: Your response must be a single, valid JSON object",
            "Do NOT include any preamble, conversation filler, or markdown formatting (like ```json). "
            "Start your response with '{' and end with '}'"
        }


        try:
            response = await self.client.messages.create(
                model = model_to_use,
                max_tokens = 1024,
                temperature = temperature,
                system = full_system,
                messages = [
                    {"role" : "user", "content" : prompt}
                ]
            )

            content = response.content[0].text.strip()

            # basic clean up
            if content.startswith("```json"):
                content = content.replace("```json", "", 1).replace("```", "", 1).strip()
            elif content.startswith("```"):
                content = content.replace("```", "", 1).replace("```", "", 1).strip()

            return json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"LLM returned invalid JSON: {content[:200]}...")
            raise RuntimeError(f"Failed to parse LLM response as JSON: {e}")
        except anthropic.APIError as e:
            logger.error(f"Anthropic API Error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in LLMClient: {e}")
            raise

    @staticmethod
    def get_holdings_context(self) -> str:
        return json.dumps(BIT_CAPITAL_HOLDINGS, indent=2)
