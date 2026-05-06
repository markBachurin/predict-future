# gemini_client.py
import asyncio
import json
import logging
from typing import Any
from google import genai
from google.genai import types
from pss_config.config import settings

logger = logging.getLogger(__name__)


class GeminiAPIClient:
    def __init__(
        self,
        model: str = settings.llm_model,
        temperature: float = settings.llm_temperature,
    ):
        self.model = model
        self.temperature = temperature
        self.client = genai.Client(api_key=settings.gemini_api_key)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def get_json_completion(
        self,
        prompt: str,
        system: str = "",
        max_retries: int = 3,
        retry_delay: float = 10.0,
    ) -> Any:
        full_prompt = f"{system}\n\n{prompt}" if system else prompt
        logger.info(f"[gemini] Sending prompt ({len(full_prompt)} chars)...")

        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                response = await asyncio.to_thread(
                    self.client.models.generate_content,
                    model=self.model,
                    contents=full_prompt,
                    config=types.GenerateContentConfig(
                        temperature=self.temperature,
                        response_mime_type="application/json",
                    ),
                )

                raw = response.text.strip()
                logger.info(f"[gemini] Response (attempt {attempt}): {raw[:500]}")

                result = json.loads(raw)

                if isinstance(result, list) and len(result) == 0:
                    logger.warning(f"[gemini] Empty array on attempt {attempt}, retrying...")
                    await asyncio.sleep(retry_delay)
                    continue

                return result

            except json.JSONDecodeError as e:
                last_error = e
                logger.warning(f"[gemini] JSON parse failed attempt {attempt}: {e}. Retrying...")
                await asyncio.sleep(retry_delay)
                continue

            except Exception as e:
                last_error = e
                logger.warning(f"[gemini] Error on attempt {attempt}: {e}. Retrying...")
                await asyncio.sleep(retry_delay)
                continue

        logger.error(f"[gemini] All {max_retries} attempts failed: {last_error}")
        raise RuntimeError(f"Gemini failed after {max_retries} attempts: {last_error}")