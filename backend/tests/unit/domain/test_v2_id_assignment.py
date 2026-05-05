"""Tests for V2 ID assignment logic."""

from app.domain.decision_tree.v2_id_assignment import _format_id


class TestFormatId:
    def test_pc_format(self):
        assert _format_id("P", 1) == "P00001"
        assert _format_id("P", 137) == "P00137"
        assert _format_id("P", 99999) == "P99999"

    def test_cc_format(self):
        assert _format_id("C", 1) == "C00001"
        assert _format_id("C", 12345) == "C12345"

    def test_custom_width(self):
        assert _format_id("P", 1, width=6) == "P000001"
        assert _format_id("C", 42, width=3) == "C042"
