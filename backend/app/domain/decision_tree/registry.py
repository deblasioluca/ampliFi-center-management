"""Routine registry — manages built-in, plugin, and DSL routines (§04.6).

The registry is the central point for discovering and executing routines.
Three sources are supported:
  A. Built-in: classes decorated with @register_routine in routines/*.py
  B. Plugin: entry-points under 'cleanup.routines' group
  C. DSL: JSON rule expressions stored in cleanup.routine rows (source='dsl')
"""

from __future__ import annotations

import importlib
import pkgutil
from typing import Protocol, runtime_checkable

import structlog

from app.domain.decision_tree.context import CenterContext, RoutineResult

logger = structlog.get_logger()


@runtime_checkable
class RoutineInterface(Protocol):
    """Contract for all routines (§04.5)."""

    @property
    def code(self) -> str: ...

    @property
    def name(self) -> str: ...

    @property
    def kind(self) -> str: ...

    @property
    def tree(self) -> str | None: ...

    @property
    def params_schema(self) -> dict | None: ...

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult: ...


class RoutineRegistry:
    """Central registry for all analytical routines (§04.6)."""

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

    def codes(self) -> list[str]:
        return list(self._routines.keys())

    def reload(self) -> None:
        """Rescan builtin + plugins + DSL rows."""
        self._routines.clear()
        _scan_builtins(self)
        _scan_plugins(self)
        logger.info("routine.registry.reloaded", count=len(self._routines))


# ── Global singleton ─────────────────────────────────────────────────────

_registry = RoutineRegistry()


def get_registry() -> RoutineRegistry:
    return _registry


# ── Decorator for built-in routines ──────────────────────────────────────

_all_routine_classes: list[type] = []
_pending_registrations: list[type] = []


def register_routine(cls: type) -> type:
    """Class decorator: marks a routine for auto-registration on boot."""
    _all_routine_classes.append(cls)
    _pending_registrations.append(cls)
    return cls


# ── Discovery helpers ────────────────────────────────────────────────────


def _scan_builtins(reg: RoutineRegistry) -> None:
    """Import all modules under .routines/ to trigger @register_routine decorators."""
    from app.domain.decision_tree import routines as routines_pkg

    for _importer, modname, _ispkg in pkgutil.iter_modules(routines_pkg.__path__):
        importlib.import_module(f"app.domain.decision_tree.routines.{modname}")

    # Use all known classes (handles re-import case where decorators don't fire again)
    for cls in _all_routine_classes:
        instance = cls()
        reg.register(instance)


def _scan_plugins(reg: RoutineRegistry) -> None:
    """Discover plugins via entry-points group 'cleanup.routines'."""
    try:
        from importlib.metadata import entry_points

        eps = entry_points()
        group = (
            eps.get("cleanup.routines", [])
            if isinstance(eps, dict)
            else eps.select(group="cleanup.routines")
        )
        for ep in group:
            try:
                cls = ep.load()
                instance = cls()
                if isinstance(instance, RoutineInterface):
                    reg.register(instance)
                else:
                    logger.warning("routine.plugin.invalid", name=ep.name)
            except Exception:
                logger.exception("routine.plugin.load_error", name=ep.name)
    except Exception:
        logger.debug("routine.plugin.no_entry_points")


def boot_registry() -> RoutineRegistry:
    """Initialize the registry on application startup."""
    _registry.reload()
    return _registry
