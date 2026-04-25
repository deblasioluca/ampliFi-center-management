"""Routine registry — manages built-in, plugin, and DSL routines (section 04.6)."""

from __future__ import annotations

from typing import Protocol

import structlog

logger = structlog.get_logger()


class RoutineInterface(Protocol):
    """Contract for all routines."""

    @property
    def code(self) -> str: ...

    @property
    def name(self) -> str: ...

    @property
    def kind(self) -> str: ...

    @property
    def tree(self) -> str | None: ...

    def evaluate(self, features: dict, params: dict) -> dict: ...


class RoutineRegistry:
    """Central registry for all analytical routines."""

    def __init__(self) -> None:
        self._routines: dict[str, RoutineInterface] = {}

    def register(self, routine: RoutineInterface) -> None:
        self._routines[routine.code] = routine
        logger.info("routine.registered", code=routine.code, kind=routine.kind)

    def get(self, code: str) -> RoutineInterface | None:
        return self._routines.get(code)

    def list(self, kind: str | None = None, tree: str | None = None) -> list[RoutineInterface]:
        routines = list(self._routines.values())
        if kind:
            routines = [r for r in routines if r.kind == kind]
        if tree:
            routines = [r for r in routines if r.tree == tree]
        return routines

    def reload(self) -> None:
        logger.info("routine.registry.reload", count=len(self._routines))


registry = RoutineRegistry()
