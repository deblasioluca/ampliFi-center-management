"""Tests for the legacy GL accounts reference endpoint.

The endpoint joins SKA1 (chart-of-accounts master) with optional SKB1
(company-code-level descriptions). These tests verify response shape
and the best-effort SKB1 lookup behaviour without spinning up a DB.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from app.api.deps import PaginationParams
from app.api.reference import list_legacy_gl_accounts


def _make_ska1(saknr: str, **overrides: object) -> SimpleNamespace:
    base = {
        "id": 1,
        "mandt": "100",
        "ktopl": "INT",
        "saknr": saknr,
        "txt20": "short text",
        "txt50": "longer descriptive text",
        "glaccount_type": "P",
        "glaccount_subtype": None,
        "func_area": None,
        "ktoks": None,
        "xbilk": None,
        "xloev": None,
        "main_saknr": None,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _make_skb1(saknr: str, bukrs: str = "1000", **overrides: object) -> SimpleNamespace:
    base = {
        "id": 1,
        "saknr": saknr,
        "bukrs": bukrs,
        "stext": "skb1 description",
        "waers": "EUR",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _mock_db(
    ska1_rows: list,
    skb1_rows: list,
    total: int | None = None,
    with_search: bool = False,
) -> MagicMock:
    """Create a minimal mock Session that returns the rows we feed in.

    The endpoint runs these statements in order:
      0. (only when search is provided) select(ExplorerDisplayConfig) -> scalar_one_or_none
      1. count(SKA1) -> scalar
      2. select(SKA1) -> scalars().all()
      3. select(SKB1) -> scalars().all()
    """
    db = MagicMock()

    results: list = []

    if with_search:
        config_result = MagicMock()
        config_result.scalar_one_or_none.return_value = None
        results.append(config_result)

    count_result = MagicMock()
    count_result.scalar.return_value = total if total is not None else len(ska1_rows)
    results.append(count_result)

    ska_result = MagicMock()
    ska_result.scalars.return_value.all.return_value = ska1_rows
    results.append(ska_result)

    skb_result = MagicMock()
    skb_result.scalars.return_value.all.return_value = skb1_rows
    results.append(skb_result)

    db.execute.side_effect = results
    return db


def _pag(page: int = 1, size: int = 50) -> PaginationParams:
    return PaginationParams(page=page, size=size)


# ── Happy path ─────────────────────────────────────────────────────────


def test_gl_endpoint_returns_basic_structure() -> None:
    db = _mock_db([_make_ska1("1000000001")], [])
    out = list_legacy_gl_accounts(db=db, pag=_pag())
    assert out["page"] == 1
    assert out["size"] == 50
    assert out["total"] == 1
    assert len(out["items"]) == 1
    item = out["items"][0]
    assert item["saknr"] == "1000000001"
    assert item["txt20"] == "short text"
    # No SKB1 match -> bukrs is None
    assert item["bukrs"] is None
    assert item["waers"] is None


def test_gl_endpoint_joins_skb1_when_available() -> None:
    db = _mock_db(
        [_make_ska1("1000000001"), _make_ska1("2000000001", id=2)],
        [_make_skb1("1000000001", bukrs="1000", waers="EUR")],
    )
    out = list_legacy_gl_accounts(db=db, pag=_pag())
    by_saknr = {it["saknr"]: it for it in out["items"]}
    assert by_saknr["1000000001"]["bukrs"] == "1000"
    assert by_saknr["1000000001"]["waers"] == "EUR"
    assert by_saknr["1000000001"]["stext_skb1"] == "skb1 description"
    # Account without an SKB1 row stays None (best-effort join)
    assert by_saknr["2000000001"]["bukrs"] is None


def test_gl_endpoint_first_skb1_per_saknr_wins() -> None:
    """If an SAKNR has multiple SKB1 rows (different bukrs), only one is exposed.

    The frontend only needs one description per account; choosing the first
    keeps the response stable and small.
    """
    db = _mock_db(
        [_make_ska1("1000000001")],
        [
            _make_skb1("1000000001", bukrs="1000", id=1, waers="EUR"),
            _make_skb1("1000000001", bukrs="2000", id=2, waers="USD"),
        ],
    )
    out = list_legacy_gl_accounts(db=db, pag=_pag())
    item = out["items"][0]
    assert item["bukrs"] == "1000"
    assert item["waers"] == "EUR"


def test_gl_endpoint_empty_result() -> None:
    db = _mock_db([], [], total=0)
    out = list_legacy_gl_accounts(db=db, pag=_pag())
    assert out["total"] == 0
    assert out["items"] == []


# ── Filter parameters ──────────────────────────────────────────────────


def test_gl_endpoint_accepts_filter_params_without_error() -> None:
    """Smoke test: all optional filters can be passed in any combination."""
    db = _mock_db([_make_ska1("1000000001")], [], with_search=True)
    out = list_legacy_gl_accounts(
        db=db,
        pag=_pag(),
        ktopl="INT",
        bukrs="1000",
        saknr="100",
        search="rent",
        scope="cleanup",
        data_category="legacy",
    )
    # The mock db doesn't actually filter — we're just verifying the function
    # accepts the parameters and doesn't blow up on the argument plumbing.
    assert "items" in out
    assert "total" in out


# ── Pagination ─────────────────────────────────────────────────────────


def test_gl_endpoint_passes_through_pagination() -> None:
    db = _mock_db([_make_ska1(f"{i:010d}") for i in range(5)], [], total=42)
    out = list_legacy_gl_accounts(db=db, pag=_pag(page=3, size=20))
    assert out["page"] == 3
    assert out["size"] == 20
    assert out["total"] == 42
