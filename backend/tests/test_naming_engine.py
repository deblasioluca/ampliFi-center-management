"""Tests for the naming convention engine (§07.5)."""

from __future__ import annotations

from app.domain.naming.engine import NamingEngine, NamingTemplate


class TestNamingEngine:
    def setup_method(self) -> None:
        self.engine = NamingEngine()

    def test_simple_template(self) -> None:
        template = NamingTemplate(
            object_type="cc",
            template="{coarea}{seq:6}",
            collision_policy="error",
            legacy_survival=False,
        )
        result = self.engine.generate(
            template=template,
            source_cctr="OLD001",
            values={"coarea": "1000"},
            existing_ids=set(),
        )
        assert result.new_id.startswith("1000")
        assert len(result.new_id) == 10  # 4 + 6

    def test_collision_skip(self) -> None:
        template = NamingTemplate(
            object_type="cc",
            template="{coarea}{seq:6}",
            collision_policy="skip",
            legacy_survival=False,
        )
        r1 = self.engine.generate(
            template=template,
            source_cctr="OLD001",
            values={"coarea": "1000"},
            existing_ids=set(),
        )
        r2 = self.engine.generate(
            template=template,
            source_cctr="OLD002",
            values={"coarea": "1000"},
            existing_ids={r1.new_id},
        )
        assert r2.new_id != r1.new_id

    def test_collision_append_suffix(self) -> None:
        template = NamingTemplate(
            object_type="cc",
            template="{coarea}{seq:4}",
            collision_policy="append_suffix",
            legacy_survival=False,
        )
        r1 = self.engine.generate(
            template=template,
            source_cctr="OLD001",
            values={"coarea": "1000"},
            existing_ids=set(),
        )
        r2 = self.engine.generate(
            template=template,
            source_cctr="OLD002",
            values={"coarea": "1000"},
            existing_ids={r1.new_id},
        )
        assert r2.new_id != r1.new_id

    def test_legacy_survival(self) -> None:
        template = NamingTemplate(
            object_type="cc",
            template="{coarea}{seq:6}",
            collision_policy="error",
            legacy_survival=True,
        )
        result = self.engine.generate(
            template=template,
            source_cctr="CC0100",
            values={"coarea": "1000"},
            existing_ids=set(),
        )
        # With legacy_survival, should keep original
        assert result.new_id == "CC0100"
        assert result.is_legacy_survival is True

    def test_batch_generation(self) -> None:
        template = NamingTemplate(
            object_type="cc",
            template="{coarea}{seq:6}",
            collision_policy="skip",
            legacy_survival=False,
        )
        centers = [
            {"source_cctr": f"OLD{i:03d}", "values": {"coarea": "1000"}}
            for i in range(5)
        ]
        results = self.engine.generate_batch(
            template=template,
            centers=centers,
            existing_ids=set(),
        )
        assert len(results) == 5
        ids = {r.new_id for r in results}
        assert len(ids) == 5  # All unique

    def test_collision_error_raises(self) -> None:
        """When collision_policy is 'error' and the generated ID already exists, raise."""
        import pytest

        template = NamingTemplate(
            object_type="cc",
            template="{coarea}{seq:6}",
            collision_policy="error",
            legacy_survival=False,
            start_range=1,
            end_range=1,  # Force only one sequence value possible
        )
        r1 = self.engine.generate(
            template=template,
            source_cctr="OLD001",
            values={"coarea": "1000"},
            existing_ids=set(),
        )
        engine2 = NamingEngine()
        with pytest.raises(ValueError, match="collision"):
            engine2.generate(
                template=template,
                source_cctr="OLD002",
                values={"coarea": "1000"},
                existing_ids={r1.new_id},
            )
