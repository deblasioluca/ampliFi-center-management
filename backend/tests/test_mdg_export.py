"""Tests for MDG export (§09.4)."""

from __future__ import annotations

from app.infra.mdg.export import export_cost_centers, export_profit_centers, export_retire_list


class TestMDGExport:
    def test_export_cost_centers(self) -> None:
        centers = [
            {
                "cctr": "CC0100",
                "coarea": "1000",
                "txtsh": "Admin",
                "txtmi": "Administration",
                "responsible": "JDOE",
                "ccode": "DE01",
                "cctrcgy": "H",
                "currency": "EUR",
                "pctr": "PC0100",
            }
        ]
        result = export_cost_centers(centers, wave_id=1)
        assert result.record_count == 1
        assert result.export_type == "cost_center"
        assert "CC0100" in result.content
        assert "1000" in result.content
        assert result.filename.startswith("MDG_CC_WAVE1_")

    def test_export_profit_centers(self) -> None:
        centers = [
            {
                "pctr": "PC0100",
                "coarea": "1000",
                "txtsh": "Sales",
                "txtmi": "Sales Dept",
                "responsible": "JDOE",
                "ccode": "DE01",
                "currency": "EUR",
            }
        ]
        result = export_profit_centers(centers, wave_id=2)
        assert result.record_count == 1
        assert result.export_type == "profit_center"
        assert "PC0100" in result.content

    def test_export_retire_list(self) -> None:
        centers = [
            {
                "cctr": "CC0900",
                "coarea": "1000",
                "txtsh": "Old Center",
                "txtmi": "To be retired",
                "responsible": "",
                "ccode": "DE01",
                "cctrcgy": "H",
                "currency": "EUR",
                "pctr": "",
            }
        ]
        result = export_retire_list(centers, wave_id=3)
        assert result.record_count == 1
        assert result.export_type == "retire"
        assert "DEACTIVATE" in result.content

    def test_empty_export(self) -> None:
        result = export_cost_centers([], wave_id=1)
        assert result.record_count == 0
