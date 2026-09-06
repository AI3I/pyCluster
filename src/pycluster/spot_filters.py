from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Callable, Iterable


@dataclass(frozen=True, slots=True)
class SpotFilterEntry:
    family: str
    action: str
    slot: int
    expr: str


@dataclass(frozen=True, slots=True)
class FilterDecision:
    allowed: bool
    reason: str
    rule: SpotFilterEntry | None = None


def validate_expression(expr: str) -> str:
    text = expr.strip()
    if not 1 <= len(text) <= 160 or any(ord(char) < 32 for char in text):
        raise ValueError('Filter expressions require 1-160 printable characters')
    selectors = {'on', 'by', 'dx', 'call', 'callsign', 'call_zone', 'call_itu', 'call_dxcc', 'call_cont', 'dx_cont', 'spotter_cont', 'by_cont', 'spotter_zone', 'by_zone', 'spotter_itu', 'by_itu', 'info'}
    for clause in re.split(r'\s+and\s+', text, flags=re.IGNORECASE):
        parts = clause.split(maxsplit=1)
        if not parts or parts[0].lower() == 'and' or clause.lower().endswith(' and'):
            raise ValueError('Incomplete AND expression')
        if parts[0].lower() in selectors | {'spotter_dxcc', 'by_dxcc'} and len(parts) < 2:
            raise ValueError('Filter condition requires a value')
    return text


def entity_matches_filter(entity: object | None, expression: str) -> bool:
    if entity is None:
        return False
    rest = str(expression or "").strip()
    if not rest:
        return False
    phrases = [part.strip().upper() for part in rest.split(",") if part.strip()]
    tokens = [part.strip().upper() for part in re.split(r"[,\s]+", rest) if part.strip()]
    wanted = phrases + tokens
    name = re.sub(r"[^A-Z0-9]+", "", str(getattr(entity, "name", "") or "").upper())
    prefix = str(getattr(entity, "prefix", "") or "").strip().upper()
    return any(item == prefix or re.sub(r"[^A-Z0-9]+", "", item) == name for item in wanted)


def is_legacy_rbn_expr(expr: str) -> bool:
    parts = str(expr or "").strip().lower().split(None, 1)
    return bool(parts and parts[0] == "rbn")


def evaluate_entries(entries: Iterable[SpotFilterEntry], matcher: Callable[[str], bool]) -> bool:
    return explain_entries(entries, matcher).allowed


def explain_entries(entries: Iterable[SpotFilterEntry], matcher: Callable[[str], bool]) -> FilterDecision:
    rules = list(entries)
    accepts = [row for row in rules if row.action == "accept"]
    matches = [row for row in rules if row.action in {"accept", "reject"} and matcher(row.expr)]
    if matches:
        matches.sort(key=lambda row: (row.slot, 0 if row.action == "reject" else 1))
        rule = matches[0]
        return FilterDecision(rule.action == "accept", "matched_rule", rule)
    return FilterDecision(not accepts, "no_accept_match" if accepts else "default_allow")


def evaluate_spot_entries(
    entries: Iterable[SpotFilterEntry],
    matcher: Callable[[str], bool],
    *,
    is_rbn: bool,
) -> bool:
    return explain_spot_entries(entries, matcher, is_rbn=is_rbn).allowed


def explain_spot_entries(
    entries: Iterable[SpotFilterEntry], matcher: Callable[[str], bool], *, is_rbn: bool,
) -> FilterDecision:
    rows = list(entries)
    spots = [row for row in rows if row.family == "spots"]
    explicit_rbn = [row for row in rows if row.family == "rbn"]
    legacy_rbn = [row for row in spots if is_legacy_rbn_expr(row.expr)]
    general = [row for row in spots if not is_legacy_rbn_expr(row.expr)]

    if not is_rbn:
        return explain_entries(general, matcher)

    scoped = explicit_rbn or legacy_rbn
    if not scoped:
        return explain_entries(general, matcher)

    # General rejects remain global policy, while general accepts describe
    # human-origin spots and must not open the automated RBN stream.
    for row in sorted(general, key=lambda item: item.slot):
        if row.action == "reject" and matcher(row.expr):
            return FilterDecision(False, "global_reject", row)
    return explain_entries(scoped, matcher)
