# Contributing

## Scope

pyCluster is a working DX cluster core, not just a protocol experiment. Contributions should improve real operator and user experience, not only internal elegance.

## Preferred Contribution Areas

- telnet usability and readability
- System Operator web improvements
- public web improvements
- node-link interoperability
- documentation
- deployment and operational tooling
- tests for real regressions

## Before Sending Changes

- keep changes focused
- prefer readable operator-facing behavior over raw internal output
- avoid unnecessary churn in established command names
- add or update tests when behavior changes
- update docs when features or operator workflows change

## Testing

At minimum, run the relevant local test slices for what you changed.

Examples:

```bash
pytest -q tests/test_web_admin.py
pytest -q tests/test_public_web.py
pytest -q tests/test_telnet_commands.py -k access
python3 -m py_compile src/pycluster/web_admin.py
```

## Style

- Python `3.11+`
- concise, operator-readable output
- ASCII by default
- maintain compatibility where it matters, but do not preserve confusing legacy behavior just because it is old
