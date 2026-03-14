# pyCluster

pyCluster is a modern DX cluster core written in Python.

It keeps the familiar telnet-style operator experience, adds a public web UI and a System Operator web console, and remains compatible with legacy cluster ecosystems such as DXSpider-family node links.

## What pyCluster Does

- serves DX-style telnet access for users and operators
- provides a public web UI for viewing and posting cluster traffic
- provides a System Operator web console for runtime, protocol, user, and peer management
- stores spots, messages, and user preferences in SQLite
- supports node linking with profile-aware behavior for legacy cluster families
- ships with deployment tooling for `systemd`-based Linux hosts
- integrates with `fail2ban` for login-abuse protection
- maintains local CTY data with optional automatic refresh from Country Files

## Where pyCluster Improves on Legacy Cluster Software

pyCluster is not just trying to mimic old command names. It is trying to keep the parts of legacy cluster software that matter while improving the parts that usually feel neglected.

Key improvements:

- cleaner telnet output and more human-readable replies
- explicit operator command namespace with `sysop/*`
- public web UI for normal users
- System Operator web console for runtime and policy management
- clearer link and protocol visibility
- per-user access matrix for telnet and web
- integrated audit and security visibility
- structured auth-failure logging with `fail2ban` support
- bundled and refreshable CTY data instead of relying on stale host copies
- Linux-first deployment with `systemd` tooling

## Current Status

pyCluster is usable today as a single-node cluster with web and telnet access, persistent storage, peer linking, and operator controls. The codebase is still evolving, but it is no longer just a prototype.

## Interfaces

### Telnet

Primary human and compatibility interface.

- user prompt: `AI3I-16> `
- sysop prompt: `AI3I-16# `
- DX-style command surface with `show/*`, `set/*`, `unset/*`, aliases, and `sysop/*`

### Public Web UI

User-facing browser interface.

- spot list and filters
- cluster view
- watch lists and recent matches
- operate tab for login and posting
- profile editing for normal users

### System Operator Web UI

Operator-facing browser console.

- node presentation and MOTD
- user and access management
- peer and link management
- protocol health and policy drops
- audit and security views

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

pycluster --config ./config/pycluster.toml serve
```

Default listeners:

- telnet: `0.0.0.0:7300`
- sysop web: `127.0.0.1:8080`
- public web: `127.0.0.1:8081`

## Deployment

Production deployment is handled through the checked-in `deploy/` scripts and `systemd` units.

Typical install:

```bash
sudo ./deploy/install.sh
sudo ./deploy/doctor.sh
```

Typical upgrade:

```bash
sudo ./deploy/upgrade.sh
sudo ./deploy/doctor.sh
```

Installed services:

- `pycluster.service`
- `pyclusterweb.service`
- `pycluster-cty-refresh.timer`

## Security

pyCluster supports:

- local callsign blocking
- per-user access controls for telnet and web
- structured auth-failure logging
- shipped `fail2ban` filters and jails
- sysop visibility for recent auth failures and current bans

## CTY Data

pyCluster ships with a bundled `cty.dat`, and install/upgrade perform a best-effort refresh from Country Files.

Manual refresh:

```bash
python3 ./scripts/update_cty.py --config ./config/pycluster.toml
```

Automatic refresh:

- `pycluster-cty-refresh.timer`

## Documentation

- [Installation](docs/installation.md)
- [Configuration](docs/configuration.md)
- [Feature Highlights](docs/feature-highlights.md)
- [Telnet Commands](docs/telnet-commands.md)
- [System Operator Web](docs/sysop-web.md)
- [Public Web UI](docs/public-web.md)
- [Node Linking](docs/node-linking.md)
- [Security](docs/security.md)
- [Operations](docs/operations.md)
- [Architecture](docs/architecture.md)
- [Roadmap](docs/pycluster-roadmap.md)
- [Project History](docs/pycluster-project-history.md)

## Developer Notes

This repo also contains compatibility research and parity artifacts gathered from live DXSpider environments. Those are useful for implementation work, but they are secondary to the user/operator docs above.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Change Log

See [CHANGELOG.md](CHANGELOG.md).
