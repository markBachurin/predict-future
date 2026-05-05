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
        response_timeout: float = 150,
        model: str = "gemini-2.5-flash-lite",
    ):
        self.gemini_cmd = gemini_cmd
        self.response_timeout = response_timeout
        self.model = model

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def get_json_completion(self, prompt: str, system: str = "") -> dict[str, Any]:
        full_prompt = self._build_prompt(prompt, system)

        logger.info(f"[gemini] Sending prompt ({len(full_prompt)} chars)...")

        try:
            process = await asyncio.create_subprocess_exec(
                self.gemini_cmd, "-p", full_prompt, "--skip-trust", "--model", self.model,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=self.response_timeout,
                )
            except asyncio.TimeoutError:
                process.kill()
                raise TimeoutError(f"Gemini did not respond within {self.response_timeout}s")

            if stderr:
                for line in stderr.decode("utf-8", errors="replace").splitlines():
                    # filter out known noisy warnings
                    if any(skip in line for skip in ["True color", "Ripgrep", "Warning"]):
                        continue
                    logger.warning(f"[gemini stderr] {line}")

            raw = stdout.decode("utf-8", errors="replace").strip()
            logger.info(f"[gemini] Raw response: {raw[:300]}")

            return self._parse_json(raw)

        except (TimeoutError, RuntimeError):
            raise
        except Exception as e:
            logger.error(f"[gemini] Unexpected error: {e}")
            raise

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