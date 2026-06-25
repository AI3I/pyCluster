from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable


@dataclass(frozen=True, slots=True)
class SpotFilterEntry:
    family: str
    action: str
    slot: int
    expr: str


def is_legacy_rbn_expr(expr: str) -> bool:
    parts = str(expr or "").strip().lower().split(None, 1)
    return bool(parts and parts[0] == "rbn")


def evaluate_entries(entries: Iterable[SpotFilterEntry], matcher: Callable[[str], bool]) -> bool:
    rules = list(entries)
    accepts = [row for row in rules if row.action == "accept"]
    matches = [row for row in rules if row.action in {"accept", "reject"} and matcher(row.expr)]
    if matches:
        matches.sort(key=lambda row: (row.slot, 0 if row.action == "reject" else 1))
        return matches[0].action == "accept"
    return not accepts


def evaluate_spot_entries(
    entries: Iterable[SpotFilterEntry],
    matcher: Callable[[str], bool],
    *,
    is_rbn: bool,
) -> bool:
    rows = list(entries)
    spots = [row for row in rows if row.family == "spots"]
    explicit_rbn = [row for row in rows if row.family == "rbn"]
    legacy_rbn = [row for row in spots if is_legacy_rbn_expr(row.expr)]
    general = [row for row in spots if not is_legacy_rbn_expr(row.expr)]

    if not is_rbn:
        return evaluate_entries(general, matcher)

    scoped = explicit_rbn or legacy_rbn
    if not scoped:
        return evaluate_entries(general, matcher)

    # General rejects remain global policy, while general accepts describe
    # human-origin spots and must not open the automated RBN stream.
    if any(matcher(row.expr) for row in general if row.action == "reject"):
        return False
    return evaluate_entries(scoped, matcher)
