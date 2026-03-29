# pyCluster

pyCluster is a modern DX cluster core written in Python.

It keeps the familiar telnet-style operator experience, adds a public web UI and a System Operator web console, and remains compatible with legacy cluster ecosystems such as DXSpider-family node links.

## 🔴 Live Demo

- public web UI: https://pycluster.ai3i.net
- public telnet listeners:
  - [pycluster.ai3i.net:7300](telnet://pycluster.ai3i.net:7300)
  - [pycluster.ai3i.net:7373](telnet://pycluster.ai3i.net:7373)
  - [pycluster.ai3i.net:8000](telnet://pycluster.ai3i.net:8000)

## ✨ Highlights

- Telnet-first DX cluster workflow with modernized operator output
- Public web UI for users and a dedicated web console for system operators
- SQLite persistence, CTY refresh tooling, and fail2ban integration
- Validated deploy path across modern Debian, Ubuntu, Fedora, and Red Hat-family Linux

## 🧭 What pyCluster Does

- serves DX-style telnet access for users and operators
- provides a public web UI for viewing and posting cluster traffic
- provides a System Operator web console for runtime, protocol, user, and peer management
- stores spots, messages, and user preferences in SQLite
- supports node linking with profile-aware behavior for legacy cluster families
- ships with deployment tooling for systemd-based Linux hosts
- integrates with fail2ban for login-abuse protection
- supports age-based cleanup for spots, messages, and bulletins
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
- structured auth-failure logging with fail2ban support
- age-based retention controls with daily cleanup
- bundled and refreshable CTY data instead of relying on stale host copies
- Linux-first deployment with systemd tooling

## 📌 Current Status

pyCluster is usable today as a single-node cluster with web and telnet access, persistent storage, peer linking, and operator controls. The codebase is still evolving, but it is no longer just a prototype.

## 🖥️ Interfaces

### Telnet

Primary human and compatibility interface.

- user prompt: `N0CALL-1> `
- sysop prompt: `N0CALL-1# `
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

## 🚀 Quick Start

Get the code with SSH:

```bash
git clone git@github.com:AI3I/pyCluster.git
cd pyCluster
```

Or with HTTPS:

```bash
git clone https://github.com/AI3I/pyCluster.git
cd pyCluster
```

Update an existing checkout:

```bash
git pull --ff-only
```

Run locally for development:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

pycluster --config ./config/pycluster.toml serve
```

Deploy on a supported Linux host:

```bash
sudo ./deploy/install.sh
sudo ./deploy/doctor.sh
```

Upgrade an existing deployment:

```bash
git pull --ff-only
sudo ./deploy/upgrade.sh
sudo ./deploy/doctor.sh
```

For upgrades from `1.0.0` to `1.0.1`, `deploy/upgrade.sh` also performs the required state conversion:

- hashes any legacy plaintext passwords still stored in `user_prefs`
- seeds `config/strings.toml` if it is missing
- preserves the existing `config/pycluster.toml`, data, and logs in place

Default listeners:

- telnet: 0.0.0.0:7300
- sysop web: 127.0.0.1:8080
- public web: 127.0.0.1:8081

## 🛠️ Deployment

Production deployment is handled through the checked-in `deploy/` scripts and systemd units.

Validated deployment targets:

- Debian 12 and 13
- Ubuntu 24.04 LTS and 25.10
- Fedora 42 and 43 with SELinux enforcing
- CentOS Stream 9 and 10 with SELinux enforcing
- AlmaLinux 8, 9, and 10 with SELinux enforcing
- Rocky Linux 8, 9, and 10 with SELinux enforcing

Deployment notes:

- `install.sh`, `upgrade.sh`, `repair.sh`, and `uninstall.sh` have been validated on the distributions above
- Fedora, CentOS Stream, AlmaLinux, and Rocky Linux installs on very small 1 GB hosts may require temporary swap during package installation; the deploy scripts now handle that automatically
- RHEL support is expected to track the validated Fedora, CentOS Stream, AlmaLinux, and Rocky Linux path, but has not yet been tested on a subscription-backed Red Hat host
- Oracle Linux is likely to work as a Red Hat-family target, but has not yet been directly validated
- Raspberry Pi OS / Raspbian is not yet validated, though 64-bit Debian- or Ubuntu-style images are the most likely to work cleanly
- Older baselines should not be attempted:
  - Debian 11
  - Ubuntu 22.04 LTS
  - CentOS 7 / RHEL 7 / Oracle Linux 7 and below
- pyCluster requires Python 3.11+, so older distro baselines without a current Python runtime are out of scope for the supported deployment path

Typical install:

```bash
sudo ./deploy/install.sh
sudo ./deploy/doctor.sh
```

<mark><strong>Initial System Operator web access uses the <code>SYSOP</code> account. The generated bootstrap password is printed prominently by the installer, written to <code>/root/pycluster-initial-sysop.txt</code>, and interactive installs pause for explicit acknowledgement so the credentials are not missed.</strong></mark>

Typical upgrade:

```bash
sudo ./deploy/upgrade.sh
sudo ./deploy/doctor.sh
```

If you are moving an existing node from `1.0.0` to `1.0.1`, run that upgrade path instead of reinstalling. The upgrader handles the `1.0.1` state conversion in place.

Installed services:

- `pycluster.service`
- `pyclusterweb.service`
- `pycluster-cty-refresh.timer`
- `pycluster-retention.timer`

## 📦 Hardware Requirements

Minimum practical deployment:

- 1 vCPU
- 1 GB RAM
- 10 GB storage
- persistent network connectivity

Recommended small production node:

- 2 vCPU
- 2 GB RAM
- 20 GB SSD-backed storage

Notes:

- SQLite works well at this scale
- reverse proxy, fail2ban, and package upgrades are more comfortable with 2 GB RAM
- very small Fedora or Red Hat-family hosts may temporarily need swap during package operations

## 🔐 Security

pyCluster supports:

- local callsign blocking
- per-user access controls for telnet and web
- structured auth-failure logging
- shipped `fail2ban` filters and jails
- imported exact-IP blocks from DXSpider `badip.local`
- sysop visibility for recent auth failures and current bans

Auth-failure log retention:

- shipped logrotate policy for `/var/log/pycluster/authfail.log`

## 🌍 CTY Data

pyCluster ships with a bundled `cty.dat`, and install/upgrade perform a best-effort refresh from Country Files.

Manual refresh:

```bash
python3 ./scripts/update_cty.py --config ./config/pycluster.toml
```

Automatic refresh:

- `pycluster-cty-refresh.timer`

## 🧹 Retention and Cleanup

pyCluster can automatically prune older operational data.

- spots, messages, and bulletins can be retained for configurable day counts
- the System Operator web UI exposes:
  - `Enable age-based cleanup`
  - per-category day values
  - `Run Cleanup Now`
- scheduled cleanup runs daily through:
  - `pycluster-retention.timer`

## 📚 Documentation

- [User Manual](docs/user-manual.md)
- [Administration Manual](docs/administration-manual.md)
- [Installation](docs/installation.md)
- [Migration](docs/migration.md)
- [Configuration](docs/configuration.md)
- [Feature Highlights](docs/feature-highlights.md)
- [Telnet Commands](docs/telnet-commands.md)
- [Telnet Command Reference](docs/telnet-command-reference.md)
- [System Operator Web](docs/sysop-web.md)
- [Public Web UI](docs/public-web.md)
- [Node Linking](docs/node-linking.md)
- [Security](docs/security.md)
- [Operations](docs/operations.md)
- [Architecture](docs/architecture.md)
- [Roadmap](docs/pycluster-roadmap.md)
- [Project History](docs/pycluster-project-history.md)

## 🙏 Credits

pyCluster is created and led by John D. Lewis, AI3I with help from ChatGPT OpenAI Codex and Anthropic Claude AI.

Special thanks for advice, assistance, consideration and testing:

- Eric Tichansky, NO3M
- Howard Leadmon, WB3FFV
- Joe Reed, N9JR

## 🤝 Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## 🕒 Change Log

See [CHANGELOG.md](CHANGELOG.md).
