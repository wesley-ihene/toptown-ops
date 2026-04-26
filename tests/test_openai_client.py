"""Tests for the minimal OpenAI adapter."""

from __future__ import annotations

from types import SimpleNamespace

from packages.llm_adapter import openai_client


def test_refine_response_text_success_returns_text(monkeypatch) -> None:
    request: dict[str, object] = {}
    client_args: dict[str, object] = {}

    class FakeResponses:
        def create(self, **kwargs):
            request.update(kwargs)
            return SimpleNamespace(output_text="Refined operational response.")

    class FakeClient:
        def __init__(self, *, api_key=None) -> None:
            client_args["api_key"] = api_key
            self.responses = FakeResponses()

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(openai_client, "OpenAI", FakeClient)
    context: dict[str, object] = {
        "response_type": "accepted_ack",
        "channel": "whatsapp",
    }

    result = openai_client.refine_response_text(
        "This is a long enough base response text to allow rewriting.",
        context,
    )

    assert result == "Refined operational response."
    assert context["_llm_adapter_status"] == "success"
    assert client_args["api_key"] == "test-key"
    assert request["model"] == "gpt-4o-mini"
    assert request["timeout"] == 10
    assert request["input"] == [
        {
            "role": "user",
            "content": openai_client._user_prompt(
                base_text="This is a long enough base response text to allow rewriting.",
                context=context,
            ),
        }
    ]


def test_refine_response_text_nested_content_returns_text(monkeypatch) -> None:
    class FakeResponses:
        def create(self, **kwargs):
            del kwargs
            return SimpleNamespace(
                output_text="",
                output=[
                    SimpleNamespace(
                        content=[
                            SimpleNamespace(type="output_text", text="Nested operational response.")
                        ]
                    )
                ],
            )

    class FakeClient:
        def __init__(self, *, api_key=None) -> None:
            del api_key
            self.responses = FakeResponses()

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(openai_client, "OpenAI", FakeClient)
    context: dict[str, object] = {}

    result = openai_client.refine_response_text(
        "This is a long enough base response text to allow rewriting.",
        context,
    )

    assert result == "Nested operational response."
    assert context["_llm_adapter_status"] == "success"


def test_refine_response_text_api_failure_returns_none(monkeypatch) -> None:
    class FakeResponses:
        def create(self, **kwargs):
            del kwargs
            raise RuntimeError("api failed")

    class FakeClient:
        def __init__(self, *, api_key=None) -> None:
            del api_key
            self.responses = FakeResponses()

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(openai_client, "OpenAI", FakeClient)
    context: dict[str, object] = {}

    result = openai_client.refine_response_text(
        "This is a long enough base response text to allow rewriting.",
        context,
    )

    assert result is None
    assert context["_llm_adapter_status"] == "failure"
