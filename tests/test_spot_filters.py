from pycluster.spot_filters import SpotFilterEntry as Rule, explain_spot_entries, evaluate_spot_entries
from pycluster.spot_filters import validate_expression
import pytest


@pytest.mark.parametrize('expression', ['', 'by', 'spotter_dxcc', 'by AI3I-99 and', 'by AI3I-99 and on', 'by\nAI3I-99', 'x' * 161])
def test_incomplete_filter_expressions_are_rejected(expression):
    with pytest.raises(ValueError):
        validate_expression(expression)


def test_compound_entity_filter_and_legacy_literal_are_valid():
    expression = 'spotter_dxcc United States,Canada and on 20m'
    assert validate_expression(expression) == expression
    assert validate_expression('legacy literal') == 'legacy literal'


def test_rbn_global_reject_overrides_explicit_accept():
    reject = Rule('spots', 'reject', 9, 'by AI3I-99')
    rows = [Rule('rbn', 'accept', 0, 'all'), reject]
    result = explain_spot_entries(rows, lambda expr: True, is_rbn=True)
    assert not result.allowed
    assert result.reason == 'global_reject'
    assert result.rule == reject


def test_rule_explanation_preserves_precedence_and_defaults():
    rows = [Rule('spots', 'accept', 3, 'match'), Rule('spots', 'reject', 3, 'match')]
    result = explain_spot_entries(rows, lambda expr: expr == 'match', is_rbn=False)
    assert result.rule == rows[1]
    assert not result.allowed
    result = explain_spot_entries(rows, lambda expr: False, is_rbn=False)
    assert result.reason == 'no_accept_match'
    assert not result.allowed
    assert explain_spot_entries([], lambda expr: False, is_rbn=False).allowed


def test_explicit_rbn_rules_do_not_open_ordinary_stream():
    rows = [Rule('spots', 'accept', 2, 'miss'), Rule('rbn', 'accept', 0, 'match')]
    matcher = lambda expr: expr == 'match'
    assert not evaluate_spot_entries(rows, matcher, is_rbn=False)
    assert evaluate_spot_entries(rows, matcher, is_rbn=True)
