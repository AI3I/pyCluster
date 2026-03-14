# Contributing

Thanks for contributing to pyCluster.

pyCluster is a working DX cluster core, not just a protocol experiment. Good contributions improve actual operator and user outcomes, not only internal elegance.

## Project Priorities

pyCluster values:

- operator-readable behavior
- practical interoperability with legacy cluster ecosystems
- Linux-first deployment that is repeatable and supportable
- honest documentation
- tests that protect real behavior

Areas that are especially useful:

- telnet usability and readability
- System Operator web improvements
- public web improvements
- node-link interoperability
- deployment and operational tooling
- documentation
- regression tests for real workflows

## Contribution Guidelines

Before sending changes:

- keep the change focused
- prefer readable operator-facing behavior over raw internal output
- avoid unnecessary churn in established commands unless there is a clear product reason
- preserve compatibility where wire behavior matters
- do not preserve confusing legacy behavior just because it is old
- update tests when behavior changes
- update documentation when features, workflows, deploy behavior, or support policy change

If you change deployment behavior:

- update the relevant docs in `README.md`, `docs/installation.md`, and `docs/operations.md`
- call out platform support changes explicitly
- document new services, timers, or external dependencies

If you change user or sysop workflows:

- update the relevant manual pages
- keep examples realistic and operator-focused
- prefer neutral example identities instead of site-specific private data

## Testing Expectations

At minimum, run the relevant local test slices for the area you changed.

Examples:

```bash
pytest -q tests/test_web_admin.py
pytest -q tests/test_public_web.py
pytest -q tests/test_telnet_commands.py -k access
python3 -m py_compile src/pycluster/web_admin.py
```

Common useful slices:

```bash
pytest -q tests/test_store.py
pytest -q tests/test_telnet_commands.py
pytest -q tests/test_web_admin.py
pytest -q tests/test_public_web.py
```

If you touch deploy tooling, validation on real Linux hosts is preferred over assumptions.

## Coding Style

- Python `3.11+`
- concise, operator-readable output
- ASCII by default
- comments should be sparse and useful
- keep user-visible output human-readable
- maintain compatibility where it matters, but do not preserve confusing legacy behavior just because it is old

## Documentation Style

- write for operators and deployers, not just developers
- use examples that demonstrate real workflows
- keep commands and paths explicit
- keep support claims honest and tied to what has actually been validated

## Pull Requests

A good pull request should make it easy to answer:

- what changed
- why it changed
- how it was tested
- what docs were updated
- whether deploy, compatibility, or operator behavior changed

If there are known limitations or residual risks, say so directly.
