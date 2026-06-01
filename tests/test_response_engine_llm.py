"""Tests for controlled GPT-4o mini rewrite integration in the response engine."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import packages.record_store.paths as record_paths
from packages.observability import load_daily_artifact
from apps.response_engine import worker


def test_accepted_llm_rewrite_preserves_meaning(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("CONVERSATION_LLM_ENABLED", "true")
    monkeypatch.setenv("CONVERSATION_LLM_MODE", "rewrite_only")

    def rewrite(base_text, context):
        context["_llm_adapter_status"] = "success"
        return "✅ DAY-END SALES REPORT received for Waigani, 23/04/26.\nSuccessfully processed."

    monkeypatch.setattr(worker, "refine_response_text", rewrite)

    rendered = worker.render_whatsapp_response(_accepted_context())
    observability = load_daily_artifact("conversation_llm", "2026-04-23", output_root=tmp_path)

    assert rendered["response_text"] == "✅ DAY-END SALES REPORT received for Waigani, 23/04/26.\nSuccessfully processed."
    assert observability is not None
    assert observability["summary"]["conversation_llm_calls"] == 1
    assert observability["summary"]["conversation_llm_failures"] == 0


def test_non_empty_llm_rewrite_is_used_even_without_adapter_status(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("CONVERSATION_LLM_ENABLED", "true")
    monkeypatch.setenv("CONVERSATION_LLM_MODE", "rewrite_only")

    monkeypatch.setattr(
        worker,
        "refine_response_text",
        lambda base_text, context: "Clear operational rewrite.",
    )

    rendered = worker.render_whatsapp_response(_accepted_context())
    observability = load_daily_artifact("conversation_llm", "2026-04-23", output_root=tmp_path)

    assert rendered["response_text"] == "Clear operational rewrite."
    assert observability is not None
    assert observability["summary"]["conversation_llm_calls"] == 1
    assert observability["summary"]["conversation_llm_failures"] == 0
    assert observability["summary"]["conversation_llm_fallbacks"] == 0
    assert [event["outcome"] for event in observability["events"]] == ["called", "success"]


def test_review_llm_rewrite_preserves_reason(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("CONVERSATION_LLM_ENABLED", "true")
    monkeypatch.setenv("CONVERSATION_LLM_MODE", "rewrite_only")
    calls = 0

    def rewrite(base_text, context):
        del base_text
        nonlocal calls
        calls += 1
        context["_llm_adapter_status"] = "success"
        return (
            "🟡 DAILY BALE SUMMARY received for Bena Road, 05/04/26.\n"
            "Placed in review.\n"
            "Reason: confidence needs operator review."
        )

    monkeypatch.setattr(worker, "refine_response_text", rewrite)

    rendered = worker.render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "bale_summary",
            "branch": "bena_road",
            "report_date": "2026-04-23",
            "reason": "confidence_between_review_and_accept_thresholds",
        }
    )

    assert calls == 0
    assert "⚠️ TAOP REVIEW REQUIRED" in rendered["response_text"]
    assert "Confidence between review and accept thresholds." in rendered["response_text"]
    assert "Reason: confidence needs operator review." not in rendered["response_text"]


def test_rejected_llm_rewrite_skips_when_structured_feedback_exists(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("CONVERSATION_LLM_ENABLED", "true")
    monkeypatch.setenv("CONVERSATION_LLM_MODE", "rewrite_only")

    calls = 0

    def rewrite(base_text, context):
        nonlocal calls
        calls += 1
        context["_llm_adapter_status"] = "success"
        return (
            "❌ ATTENDANCE REPORT could not be processed.\n"
            "Reason: required report fields were missing or invalid.\n"
            "Please resend using the correct format."
        )

    monkeypatch.setattr(worker, "refine_response_text", rewrite)

    rendered = worker.render_whatsapp_response(
        {
            "response_type": "rejected_fix_request",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "staff_attendance",
            "report_date": "2026-04-23",
            "reason": "validation_failed",
        }
    )

    assert calls == 0
    assert rendered["response_text"].endswith(
        "Recheck required fields and totals, then resend using the exact SOP format for Staff Attendance Report."
    )


def test_llm_failure_falls_back_to_base_text(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("CONVERSATION_LLM_ENABLED", "true")
    monkeypatch.setenv("CONVERSATION_LLM_MODE", "rewrite_only")

    def fail(base_text, context):
        del base_text, context
        return None

    monkeypatch.setattr(worker, "refine_response_text", fail)

    rendered = worker.render_whatsapp_response(_accepted_context())
    observability = load_daily_artifact("conversation_llm", "2026-04-23", output_root=tmp_path)

    assert rendered["response_text"] == "✅ DAY-END SALES REPORT received for Waigani, 23/04/26.\nProcessed successfully."
    assert observability is not None
    assert observability["summary"]["conversation_llm_calls"] == 1
    assert observability["summary"]["conversation_llm_failures"] == 0
    assert observability["summary"]["conversation_llm_fallbacks"] == 1
    assert [event["outcome"] for event in observability["events"]] == ["called", "fallback"]


def test_exception_in_llm_refine_is_recorded_as_adapter_failure(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("CONVERSATION_LLM_ENABLED", "true")
    monkeypatch.setenv("CONVERSATION_LLM_MODE", "rewrite_only")

    def explode(base_text, context):
        del base_text, context
        raise RuntimeError("adapter exploded")

    monkeypatch.setattr(worker, "refine_response_text", explode)

    rendered = worker.render_whatsapp_response(_accepted_context())
    observability = load_daily_artifact("conversation_llm", "2026-04-23", output_root=tmp_path)

    assert rendered["response_text"] == "✅ DAY-END SALES REPORT received for Waigani, 23/04/26.\nProcessed successfully."
    assert observability is not None
    assert observability["summary"]["conversation_llm_calls"] == 1
    assert observability["summary"]["conversation_llm_failures"] == 1
    assert observability["summary"]["conversation_llm_fallbacks"] == 1
    assert [event["outcome"] for event in observability["events"]] == ["called", "failed", "fallback"]


def test_replay_skips_llm_call(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("CONVERSATION_LLM_ENABLED", "true")
    monkeypatch.setenv("CONVERSATION_LLM_MODE", "rewrite_only")
    calls = 0

    def track(base_text, context):
        nonlocal calls
        calls += 1
        return base_text

    monkeypatch.setattr(worker, "refine_response_text", track)

    rendered = worker.render_whatsapp_response(
        {
            **_accepted_context(),
            "is_replay": True,
        }
    )
    observability = load_daily_artifact("conversation_llm", "2026-04-23", output_root=tmp_path)

    assert rendered["response_text"] == "✅ DAY-END SALES REPORT received for Waigani, 23/04/26.\nProcessed successfully."
    assert calls == 0
    assert observability is not None
    assert observability["summary"]["conversation_llm_skipped"] == 1


def test_command_reply_skips_llm_call(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("CONVERSATION_LLM_ENABLED", "true")
    monkeypatch.setenv("CONVERSATION_LLM_MODE", "rewrite_only")
    calls = 0

    def track(base_text, context):
        del base_text, context
        nonlocal calls
        calls += 1
        return "should not be used"

    monkeypatch.setattr(worker, "refine_response_text", track)

    rendered = worker.render_whatsapp_response(
        {
            "response_type": "command_reply",
            "response_text": "HELP TEXT",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_date": "2026-04-23",
        }
    )

    assert rendered["response_text"] == "HELP TEXT"
    assert calls == 0


def test_disabled_llm_keeps_deterministic_base_text(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.delenv("CONVERSATION_LLM_ENABLED", raising=False)
    monkeypatch.delenv("CONVERSATION_LLM_MODE", raising=False)

    rendered = worker.render_whatsapp_response(_accepted_context())
    observability = load_daily_artifact("conversation_llm", "2026-04-23", output_root=tmp_path)

    assert rendered["response_text"] == "✅ DAY-END SALES REPORT received for Waigani, 23/04/26.\nProcessed successfully."
    assert observability is not None
    assert observability["summary"]["conversation_llm_skipped"] == 1


def test_short_text_skips_llm(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("CONVERSATION_LLM_ENABLED", "true")
    monkeypatch.setenv("CONVERSATION_LLM_MODE", "rewrite_only")
    calls = 0

    def track(base_text, context):
        nonlocal calls
        calls += 1
        return base_text

    monkeypatch.setattr(worker, "refine_response_text", track)

    rendered = worker.render_whatsapp_response(
        {
            "response_type": "duplicate_notice",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_date": "2026-04-23",
        }
    )
    observability = load_daily_artifact("conversation_llm", "2026-04-23", output_root=tmp_path)

    assert rendered["response_text"] == "ℹ️ This report was already received and processed earlier.\nNo new processing was applied."
    assert calls == 0
    assert observability is not None
    assert observability["summary"]["conversation_llm_skipped"] == 1


def test_non_empty_llm_output_is_used(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("CONVERSATION_LLM_ENABLED", "true")
    monkeypatch.setenv("CONVERSATION_LLM_MODE", "rewrite_only")

    def invalid(base_text, context):
        context["_llm_adapter_status"] = "success"
        return "ok"

    monkeypatch.setattr(worker, "refine_response_text", invalid)

    rendered = worker.render_whatsapp_response(_accepted_context())
    observability = load_daily_artifact("conversation_llm", "2026-04-23", output_root=tmp_path)

    assert rendered["response_text"] == "ok"
    assert observability is not None
    assert observability["summary"]["conversation_llm_calls"] == 1
    assert observability["summary"]["conversation_llm_failures"] == 0
    assert observability["summary"]["conversation_llm_fallbacks"] == 0


def test_known_review_reason_skips_llm_and_preserves_resolved_observability_metadata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("CONVERSATION_LLM_ENABLED", "true")
    monkeypatch.setenv("CONVERSATION_LLM_MODE", "rewrite_only")
    calls = 0

    def track(base_text, context):
        del base_text, context
        nonlocal calls
        calls += 1
        return "should not run"

    monkeypatch.setattr(worker, "refine_response_text", track)

    rendered = worker.render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "routing",
            "reason": "mixed_report_split_not_safe",
            "feedback_context": {
                "raw_text": "\n".join(
                    [
                        "Supervisor Control Report",
                        "Branch: Waigani Branch",
                        "Date: 07/04/2026",
                        "Cash variance: No",
                    ]
                ),
            },
        }
    )
    observability = load_daily_artifact("conversation_llm", "2026-04-07", output_root=tmp_path)

    assert calls == 0
    assert "Mixed content could not be safely split." in rendered["response_text"]
    assert "Send one report family per WhatsApp message and resend." in rendered["response_text"]
    assert observability is not None
    assert observability["summary"]["conversation_llm_skipped"] == 1
    assert observability["events"] == [
        {
            "outcome": "skipped",
            "response_type": "review_ack",
            "channel": "whatsapp",
            "branch": "waigani",
            "report_type": "supervisor_control",
            "report_date": "2026-04-07",
            "replay_suppressed": False,
            "reason": "structured_feedback",
        }
    ]


def test_known_review_reasons_skip_llm_calls(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("CONVERSATION_LLM_ENABLED", "true")
    monkeypatch.setenv("CONVERSATION_LLM_MODE", "rewrite_only")
    calls = 0

    def track(base_text, context):
        del base_text, context
        nonlocal calls
        calls += 1
        return "should not run"

    monkeypatch.setattr(worker, "refine_response_text", track)

    current_report_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cases = [
        (
            {
                "response_type": "review_ack",
                "channel": "whatsapp",
                "should_reply": True,
                "is_replay": False,
                "report_type": "sales_income",
                "branch": "waigani",
                "report_date": "2026-04-23",
                "reason": "mixed_child_requires_review",
            },
            "2026-04-23",
        ),
        (
            {
                "response_type": "review_ack",
                "channel": "whatsapp",
                "should_reply": True,
                "is_replay": False,
                "report_type": "sales_income",
                "branch": "waigani",
                "report_date": "2026-04-23",
                "reason": "mixed_report_split_not_safe",
            },
            "2026-04-23",
        ),
        (
            {
                "response_type": "review_ack",
                "channel": "whatsapp",
                "should_reply": True,
                "is_replay": False,
                "report_type": "staff_attendance",
                "report_date": "2026-04-23",
                "reason": None,
                "feedback_context": {
                    "validation": {
                        "reason_codes": ["missing_branch"],
                        "rejections": [{"reason_code": "missing_branch", "reason_detail": "Branch missing."}],
                    },
                },
            },
            "2026-04-23",
        ),
        (
            {
                "response_type": "review_ack",
                "channel": "whatsapp",
                "should_reply": True,
                "is_replay": False,
                "report_type": "staff_attendance",
                "branch": "waigani",
                "reason": None,
                "feedback_context": {
                    "validation": {
                        "reason_codes": ["missing_report_date"],
                        "rejections": [{"reason_code": "missing_report_date", "reason_detail": "Date missing."}],
                    },
                },
            },
            current_report_date,
        ),
        (
            {
                "response_type": "review_ack",
                "channel": "whatsapp",
                "should_reply": True,
                "is_replay": False,
                "report_type": "supervisor_control",
                "branch": "waigani",
                "report_date": "2026-04-23",
                "reason": None,
                "feedback_context": {
                    "validation": {
                        "reason_codes": ["missing_fields"],
                        "rejections": [{"reason_code": "missing_fields", "reason_detail": "Missing fields."}],
                    },
                    "warnings": [{"code": "missing_fields", "message": "Missing fields."}],
                },
            },
            "2026-04-23",
        ),
    ]

    for context, expected_date in cases:
        rendered = worker.render_whatsapp_response(context)
        observability = load_daily_artifact("conversation_llm", expected_date, output_root=tmp_path)
        assert rendered["response_text"]
        assert observability is not None
        assert observability["summary"]["conversation_llm_skipped"] >= 1

    assert calls == 0


def test_conversation_llm_enabled_true_env_is_enabled(monkeypatch) -> None:
    monkeypatch.setenv("CONVERSATION_LLM_ENABLED", "true")

    assert worker._conversation_llm_enabled() is True


def test_conversation_llm_enabled_numeric_one_is_enabled(monkeypatch) -> None:
    monkeypatch.setenv("CONVERSATION_LLM_ENABLED", "1")

    assert worker._conversation_llm_enabled() is True


def test_conversation_llm_enabled_unset_is_disabled(monkeypatch) -> None:
    monkeypatch.delenv("CONVERSATION_LLM_ENABLED", raising=False)

    assert worker._conversation_llm_enabled() is False


def test_wrong_llm_mode_disables_rewrite(monkeypatch) -> None:
    monkeypatch.setenv("CONVERSATION_LLM_ENABLED", "true")
    monkeypatch.setenv("CONVERSATION_LLM_MODE", "draft")

    assert worker._llm_skip_reason(
        base_text="x" * 40,
        response_context=_accepted_context(),
    ) == "llm_mode_off"


def _accepted_context() -> dict[str, object]:
    return {
        "response_type": "accepted_ack",
        "channel": "whatsapp",
        "should_reply": True,
        "is_replay": False,
        "report_type": "sales_income",
        "branch": "waigani",
        "report_date": "2026-04-23",
    }


def _patch_environment(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")
    monkeypatch.delenv("CONVERSATION_LLM_ENABLED", raising=False)
    monkeypatch.delenv("CONVERSATION_LLM_MODE", raising=False)
    monkeypatch.delenv("TOPTOWN_CONVERSATION_LLM_ENABLED", raising=False)
    monkeypatch.delenv("TOPTOWN_CONVERSATION_LLM_MODE", raising=False)
    worker._LLM_REWRITE_CACHE.clear()
