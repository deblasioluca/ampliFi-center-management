"""Tests for the email engine (§09, §11)."""

from __future__ import annotations

from app.infra.email.engine import TEMPLATES, EmailEngine, _render_template


class TestEmailEngine:
    def test_render_template(self) -> None:
        subject, body = _render_template(
            "review_invitation",
            {
                "reviewer_name": "John Doe",
                "wave_name": "Wave Q1 2026",
                "review_url": "https://example.com/review/abc123",
                "scope_name": "EMEA Scope",
                "item_count": 50,
                "expires_at": "2026-05-01",
            },
        )
        assert "John Doe" in body
        assert "Wave Q1 2026" in body
        assert "Wave Q1 2026" in subject

    def test_render_all_templates(self) -> None:
        assert len(TEMPLATES) > 0
        for name in TEMPLATES:
            assert isinstance(name, str)
            assert "subject" in TEMPLATES[name]
            assert "body" in TEMPLATES[name]

    def test_engine_creation(self) -> None:
        engine = EmailEngine(host="localhost", port=1025)
        assert engine._host == "localhost"
        assert engine._port == 1025

    def test_unknown_template_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Unknown email template"):
            _render_template("nonexistent_template", {})
