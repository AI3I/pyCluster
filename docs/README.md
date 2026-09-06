# Documentation

This index distinguishes current operator documentation from generated or
historical compatibility research.

## Start Here

- [Installation](installation.md): development, production, upgrade, repair,
  reverse proxy, firewall, and first checks
- [Configuration](configuration.md): effective TOML settings and listener rules
- [User Manual](user-manual.md): telnet and public-web workflows
- [Administration Manual](administration-manual.md): day-to-day SysOp workflows
- [Operations](operations.md): services, backups, diagnostics, and maintenance
- [Security](security.md): authentication, exposure, logging, and hardening

## Interface References

- [Telnet Commands](telnet-commands.md): concise command introduction
- [Telnet Command Reference](telnet-command-reference.md): detailed syntax
- [System Operator Web](sysop-web.md): SysOp console controls and status
- [Public Web UI](public-web.md): user-facing pages and account behavior
- [Node Linking](node-linking.md): peer setup, families, health, and `PY` exchange
- [Migration](migration.md): supported DXSpider data migration

## Design And Direction

- [Architecture](architecture.md): component and trust boundaries
- [Command Specification](command-specification.md): command support policy
- [Feature Highlights](feature-highlights.md): current product overview
- [Roadmap](pycluster-roadmap.md): forward-looking direction
- [Future Feature Concepts](future-feature-concepts.md): unscheduled design
  notes for node discovery, optional identity, compatible authentication,
  graphical filters, and PY security advisories
- [Changelog](../CHANGELOG.md): released and development changes

## Compatibility Research

These files are evidence and engineering aids, not claims of exact behavioral
parity:

- [DXSpider Command Catalog](dxspider-command-catalog.md): a dated source-node
  inventory used as an audit input
- [DXSpider Command Parity Matrix](dxspider-parity-matrix.md): generated command
  path coverage against that catalog
- [DXCluster Compatibility Matrix](compatibility-matrix.md): an archived early
  capture of one DXSpider node and fixture set

Generated reports preserve their source snapshot dates. Re-run their generator
or collection workflow rather than hand-editing measured counts.

## Maintenance Rules

- GitHub issues and milestones define scheduled work.
- `CHANGELOG.md` records shipped behavior.
- The roadmap describes direction and should not duplicate issue-level status.
- Examples must use runtime paths and current configuration names.
- New user-facing behavior belongs in the relevant interface manual.
- New deployment behavior belongs in Installation, Configuration, Operations,
  or Security as appropriate.
