"""Naming convention engine (§07.5).

Template-based naming for target cost centers and profit centers.
Supports placeholders, legacy survival rules, collision policies,
and sequence-based numbering.

Templates use placeholders like:
    {coarea}{ccode}{seq:4}    → "1000DE000001"
    {prefix}{seq:6}           → "CC000001"
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import structlog

logger = structlog.get_logger()


@dataclass
class NamingTemplate:
    """A naming convention template."""

    object_type: str  # cc | pc | wbs
    template: str  # e.g. "{coarea}{seq:6}"
    prefix: str = ""
    coarea: str = ""
    start_range: int = 1
    end_range: int = 999999
    collision_policy: str = "skip"  # skip | error | append_suffix
    legacy_survival: bool = True  # if True, KEEP centers retain original ID


@dataclass
class GeneratedName:
    """Result of applying a naming convention."""

    object_type: str
    new_id: str
    source_cctr: str
    is_legacy_survival: bool = False
    sequence_used: int | None = None


_PLACEHOLDER_RE = re.compile(r"\{(\w+)(?::(\d+))?\}")


def _format_template(template: str, values: dict, seq: int) -> str:
    """Replace placeholders in template with values."""

    def replacer(match: re.Match) -> str:
        name = match.group(1)
        width = match.group(2)
        if name == "seq":
            w = int(width) if width else 6
            return str(seq).zfill(w)
        val = str(values.get(name, ""))
        if width:
            return val.ljust(int(width))[: int(width)]
        return val

    return _PLACEHOLDER_RE.sub(replacer, template)


class NamingEngine:
    """Generates IDs for target objects based on naming conventions."""

    def __init__(self) -> None:
        self._sequences: dict[str, int] = {}  # key → next value

    def _seq_key(self, object_type: str, coarea: str, prefix: str) -> str:
        return f"{object_type}:{coarea}:{prefix}"

    def get_next_seq(self, key: str, start: int = 1) -> int:
        if key not in self._sequences:
            self._sequences[key] = start
        val = self._sequences[key]
        self._sequences[key] = val + 1
        return val

    def set_sequence(self, key: str, value: int) -> None:
        self._sequences[key] = value

    def generate(
        self,
        template: NamingTemplate,
        source_cctr: str,
        values: dict,
        existing_ids: set[str] | None = None,
    ) -> GeneratedName:
        """Generate a new ID for a target object.

        Args:
            template: The naming template to apply
            source_cctr: The legacy cost center ID
            values: Dict with coarea, ccode, prefix, etc.
            existing_ids: Set of already-used IDs for collision checking
        """
        existing = existing_ids or set()

        # Legacy survival: KEEP centers retain original ID
        if template.legacy_survival and source_cctr:
            return GeneratedName(
                object_type=template.object_type,
                new_id=source_cctr,
                source_cctr=source_cctr,
                is_legacy_survival=True,
            )

        key = self._seq_key(
            template.object_type,
            template.coarea or values.get("coarea", ""),
            template.prefix or values.get("prefix", ""),
        )

        max_attempts = template.end_range - template.start_range + 1
        for _ in range(min(max_attempts, 10000)):
            seq = self.get_next_seq(key, template.start_range)

            if seq > template.end_range:
                raise ValueError(
                    f"Naming sequence exhausted for {key}: "
                    f"exceeded range {template.start_range}-{template.end_range}"
                )

            new_id = _format_template(template.template, values, seq)

            if new_id not in existing:
                return GeneratedName(
                    object_type=template.object_type,
                    new_id=new_id,
                    source_cctr=source_cctr,
                    sequence_used=seq,
                )

            # Collision handling
            if template.collision_policy == "error":
                raise ValueError(f"Naming collision: {new_id} already exists")
            elif template.collision_policy == "append_suffix":
                base_id = new_id
                for suffix in range(1, 100):
                    suffixed = f"{base_id}_{suffix}"
                    if suffixed not in existing:
                        return GeneratedName(
                            object_type=template.object_type,
                            new_id=suffixed,
                            source_cctr=source_cctr,
                            sequence_used=seq,
                        )
            # skip: try next sequence number

        raise ValueError(f"Could not generate unique ID after max attempts for {key}")

    def generate_batch(
        self,
        template: NamingTemplate,
        centers: list[dict],
        existing_ids: set[str] | None = None,
    ) -> list[GeneratedName]:
        """Generate IDs for a batch of centers."""
        existing = set(existing_ids) if existing_ids else set()
        results: list[GeneratedName] = []

        for center in centers:
            result = self.generate(
                template,
                source_cctr=center.get("cctr", ""),
                values=center,
                existing_ids=existing,
            )
            existing.add(result.new_id)
            results.append(result)

        return results
