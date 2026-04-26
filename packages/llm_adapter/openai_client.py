"""Minimal OpenAI adapter for controlled conversation rewrite and intent classification."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover - exercised via fallback behavior
    OpenAI = None  # type: ignore[assignment]


SYSTEM_PROMPT = """You rewrite operational system messages.

RULES:
- Do NOT change meaning
- Do NOT add new facts
- Do NOT remove decisions
- Keep responses short and clear
- No conversational fluff
- No explanations
- Return ONLY the improved message"""

INTENT_SYSTEM_PROMPT = """You classify user intent for an operations system.

Rules:
- Only choose from:
  help, status, format_sales, format_attendance, why_rejected
- Do NOT create new intents
- Do NOT answer the user
- Return JSON only:
  { "intent": "...", "confidence": 0.0-1.0 }"""

logger = logging.getLogger(__name__)
_CLIENT: Any | None = None
_INTENT_MODEL = "gpt-4o-mini"


def refine_response_text(base_text: str, context: dict[str, Any] | None = None) -> str | None:
    """Return one clarified rewrite or None on any adapter failure."""

    if context is None:
        context = {}

    if len(base_text) < 40:
        context["_llm_adapter_status"] = "skipped_short"
        return base_text

    api_key = os.environ.get("OPENAI_API_KEY")
    if OpenAI is None or not api_key:
        context["_llm_adapter_status"] = "failure"
        return None

    try:
        client = OpenAI(api_key=api_key)
        context["_llm_adapter_status"] = "pending"
        response = client.responses.create(
            model="gpt-4o-mini",
            input=[
                {
                    "role": "user",
                    "content": _user_prompt(base_text=base_text, context=context),
                }
            ],
            timeout=10,
        )
        logger.warning(f"LLM RAW RESPONSE TYPE: {type(response)}")
        logger.warning(f"LLM RAW RESPONSE: {response}")
        logger.warning(f"LLM output_text: {getattr(response, 'output_text', None)}")
        logger.warning(f"LLM output field: {getattr(response, 'output', None)}")
        text = _extract_text(response)
        if text is not None:
            context["_llm_adapter_status"] = "success"
            return text
        if hasattr(response, "model_dump"):
            logger.warning("LLM adapter empty response: %s", response.model_dump())
        else:
            logger.warning("LLM adapter empty response: %r", response)
        context["_llm_adapter_status"] = "failure"
        return None
    except Exception:
        logger.exception("LLM adapter failure")
        context["_llm_adapter_status"] = "failure"
        return None


def classify_user_intent_json(normalized_message: str) -> str | None:
    """Return one raw JSON classification string or None on any failure."""

    client = _get_client()
    if client is None:
        return None

    try:
        response = client.responses.create(
            model=_INTENT_MODEL,
            instructions=INTENT_SYSTEM_PROMPT,
            input=_intent_user_prompt(normalized_message),
            max_output_tokens=50,
            temperature=0.0,
        )
        return _extract_text(response)
    except Exception as exc:
        logger.warning("LLM adapter failure: %s", exc)
        return None


def _user_prompt(*, base_text: str, context: dict[str, Any]) -> str:
    minimal_context = {
        "response_type": context.get("response_type"),
        "channel": context.get("channel"),
        "reason": context.get("reason"),
        "report_type": context.get("report_type"),
        "branch": context.get("branch"),
    }
    return (
        "Rewrite this message more clearly without changing meaning.\n\n"
        f"Message:\n{base_text}\n\n"
        f"Context:\n{json.dumps(minimal_context, sort_keys=True, ensure_ascii=True)}"
    )


def _intent_user_prompt(normalized_message: str) -> str:
    return f"Message:\n{normalized_message}"


def _extract_text(response: Any) -> str | None:
    try:
        output_text = _field(response, "output_text")
        if isinstance(output_text, str):
            cleaned = output_text.strip()
            if cleaned:
                return cleaned

        output = _field(response, "output")
        if isinstance(output, list):
            for item in output:
                content = _field(item, "content")
                if not isinstance(content, list):
                    continue
                for part in content:
                    text = _field(part, "text")
                    if isinstance(text, str):
                        cleaned = text.strip()
                        if cleaned:
                            return cleaned
    except Exception:
        pass
    return None


def _field(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def _get_client() -> Any | None:
    global _CLIENT

    if OpenAI is None:
        return None
    if not os.environ.get("OPENAI_API_KEY"):
        return None
    if _CLIENT is not None:
        return _CLIENT
    try:
        _CLIENT = OpenAI()
    except Exception as exc:
        logger.warning("LLM adapter failure: %s", exc)
        return None
    return _CLIENT
