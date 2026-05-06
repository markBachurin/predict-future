import asyncio
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class GeminiCLIClient:
    """
    Runs Gemini CLI as a subprocess per prompt using -p --skip-trust.
    No persistent session needed — simpler and more reliable.
    """

    def __init__(
        self,
        gemini_cmd: str = "gemini",
        response_timeout: float = 300,
        model: str = "gemini-2.5-flash-lite",
    ):
        self.gemini_cmd = gemini_cmd
        self.response_timeout = response_timeout
        self.model = model

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
    ) -> dict[str, Any]:
        full_prompt = self._build_prompt(prompt, system)
        logger.info(f"[gemini] Sending prompt ({len(full_prompt)} chars)...")

        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                process = await asyncio.create_subprocess_exec(
                    self.gemini_cmd, "-p", full_prompt, "--skip-trust", "--model", self.model,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )

                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=self.response_timeout,
                )

                if stderr:
                    for line in stderr.decode("utf-8", errors="replace").splitlines():
                        if any(skip in line for skip in ["True color", "Ripgrep", "Warning"]):
                            continue
                        logger.warning(f"[gemini stderr] {line}")

                raw = stdout.decode("utf-8", errors="replace").strip()
                logger.info(f"[gemini] Raw response (attempt {attempt}): {raw[:500]}")

                # Catch empty response and retry
                if not raw:
                    logger.warning(f"[gemini] Empty response on attempt {attempt}, retrying in {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                    continue

                result = self._parse_json(raw)

                # Catch empty array and retry
                if isinstance(result, list) and len(result) == 0:
                    logger.warning(f"[gemini] Empty array on attempt {attempt}, retrying in {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                    continue

                return result

            except asyncio.TimeoutError as e: # Catch timeout from wait_for
                process.kill()
                await process.wait()
                last_error = e
                logger.warning(f"[gemini] Timeout on attempt {attempt}: {e}. Retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                continue # Continue to the next attempt

            except RuntimeError as e: # Catch JSON parse errors
                # JSON parse failure — likely truncated response
                last_error = e
                logger.warning(f"[gemini] Parse failed on attempt {attempt}: {e}. Retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                continue

            except Exception as e: # Catch any other unexpected errors
                last_error = e
                logger.error(f"[gemini] Unexpected error on attempt {attempt}: {e}")
                raise

        logger.error(f"[gemini] All {max_retries} attempts failed. Last error: {last_error}")
        raise RuntimeError(f"Gemini failed after {max_retries} attempts: {last_error}")

    # private

    def _parse_json(self, raw: str) -> dict[str, Any]:
        cleaned = raw
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            logger.error(f"[gemini] Invalid JSON: {cleaned[:300]}")
            raise RuntimeError(f"Gemini returned invalid JSON: {e}") from e

    @staticmethod
    def _build_prompt(user_prompt: str, system: str) -> str:
        lines = []
        if system:
            lines.append(f"[CONTEXT]\n{system}\n")
        lines.append("[TASK]")
        lines.append(user_prompt)
        lines.append(
            "\n[OUTPUT INSTRUCTIONS]\n"
            "You MUST respond ONLY with valid JSON — either a single object or an array. No preamble, no explanation, no markdown fences."
        )
        return "\n".join(lines)