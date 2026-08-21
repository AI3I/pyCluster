# pyCluster

pyCluster is a modern DX cluster core written in Python.

It keeps the familiar telnet-style operator experience, adds a public web UI and a System Operator web console, and remains compatible with legacy cluster ecosystems such as DXSpider-family node links.

## 🔴 Live Demo

- public web UI: https://pycluster.ai3i.net
- public telnet listeners:
  - [pycluster.ai3i.net:7300](telnet://pycluster.ai3i.net:7300)
  - [pycluster.ai3i.net:7373](telnet://pycluster.ai3i.net:7373)
  - [pycluster.ai3i.net:8000](telnet://pycluster.ai3i.net:8000)

## Community

- Groups.io: https://groups.io/g/pycluster
- Slack: https://pyclusterapp.slack.com/

The groups.io list and Slack workspace are good places to sign up, collaborate, discuss operations, and coordinate testing.

## ✨ Highlights

- Telnet-first DX cluster workflow with modernized operator output
- Public web UI for users and a dedicated web console for system operators
- SQLite persistence, CTY and wpxloc refresh tooling, and fail2ban integration
- Validated deploy path across modern Debian, Ubuntu, Fedora, and Red Hat-family Linux
- Intended for standalone deployment on a clean Linux host, VPS, Raspberry Pi, or dedicated physical system

## 🧭 What pyCluster Does

- serves DX-style telnet access for users and operators
- provides a public web UI for viewing and posting cluster traffic
- provides a System Operator web console for runtime, protocol, user, and peer management
- stores spots, messages, and user preferences in SQLite
- supports node linking with profile-aware behavior for legacy cluster families
- ships with deployment tooling for systemd-based Linux hosts
- integrates with fail2ban for login-abuse protection
- supports age-based cleanup for spots, messages, and bulletins
- maintains local `CTY.DAT` and `wpxloc.raw` data with optional automatic refresh from Country Files

## Standalone Deployment Model

pyCluster is intended to be installed as a standalone product on a clean system.

- recommended targets are a fresh VPS, Raspberry Pi, mini PC, VM, or dedicated physical server running a supported Linux distribution
- the supported deployment model is one host dedicated to pyCluster and its own bundled services
- do not plan around co-mingling pyCluster with unrelated products, control panels, large application stacks, or hand-managed service bundles on the same machine
- nginx does not have to run on the pyCluster host; a dedicated pyCluster VM can publish its web listeners to a separately managed reverse proxy on a trusted network
- when nginx runs locally, use the checked-in setup helper rather than merging generated files into an unrelated local web stack
- if you want a predictable install, upgrade, repair, and support path, start with a clean host rather than trying to layer pyCluster into an already busy system

The deployment scripts, service layout, runtime paths, and operator assumptions are written around pyCluster owning its application host cleanly. The reverse proxy may be local or external; external access must be enabled explicitly because both web listeners bind to loopback by default.

## Where pyCluster Improves on Legacy Cluster Software

pyCluster is not just trying to mimic old command names. It is trying to keep the parts of legacy cluster software that matter while improving the parts that usually feel neglected.

Key improvements:

- cleaner telnet output and more human-readable replies
- explicit operator command namespace with `sysop/*`
- public web UI for normal users
- System Operator web console for runtime and policy management
- clearer link and protocol visibility
- more protective routing and duplicate-handling behavior built into the core engine
- per-user access matrix for telnet and web
- integrated audit and security visibility
- structured auth-failure logging with fail2ban support
- age-based retention controls with daily cleanup
- refresh-managed CTY data instead of relying on stale host copies
- Linux-first deployment with systemd tooling

## Less Manual Admin Work

pyCluster is designed to reduce the amount of defensive cluster administration that older systems often push onto the operator.

In practice that means:

- duplicate and loop-resistant behavior is handled primarily in core logic rather than depending on heavy manual route-filter tuning
- in normal deployments you can usually link to multiple partner nodes without first writing special defensive route filters
- duplicate suppression, routing protections, and peer-state handling are intended to make multi-link operation work safely by default
- peer cleanup, policy-drop accounting, and protocol-health visibility are built in
- operators can still apply filters and policy controls when needed, but normal operation should not require constant route-filter micromanagement
- the goal is safer default behavior with fewer admin headaches, not recreating a large manual-maintenance burden

## 📌 Current Status

pyCluster is usable today as a single-node cluster with web and telnet access, persistent storage, peer linking, and operator controls. The codebase is still evolving, but it is no longer just a prototype.

Current release: `1.0.13`

Recent highlights in `1.0.13`:

- RBN feed ingestion is live-only and local, with bounded in-memory delivery, ten-second aggregation, nearby-frequency grouping, and respot suppression; it neither grows the historical spot database nor forwards RBN reports to cluster peers
- System Operator user management includes a locked-account view alongside blocked users
- registration-required nodes keep telnet and public requests in the approval queue until a System Operator approves them; nodes without that requirement activate public accounts after email verification
- deleted/denied user records clean up stale registration and MFA state before a callsign-SSID can be reused
- filtered `show/mydx` searches deeper durable spot history under high-volume RBN conditions
- RBN/Skimmer spots can be identified, filtered, summarized with `show/rbn`, and ingested from an optional direct telnet feed
- telnet RBN delivery is user opt-in with `set/rbn`, while `show/dx` stays focused on traditional DX spots
- public websocket, spot, statistics, and leaderboard views apply the same database-backed RBN preference and filters
- public web filters persist to the shared backend filter table used by telnet
- Skimmer spotter suffixes such as `-#` are accepted when receiving live RBN-style spots
- public web and telnet self-registration validate ham-style callsigns before creating registration records
- telnet self-registration verifies email before sysop approval when SMTP is configured, and expired verification codes tell the user to rerun `REGISTER`
- node-link peers receive fresh PC18 identity advertisements on outbound reconnects without duplicating the DXSpider transport handshake
- optional pyCluster-only capability negotiation, decentralized topology, and read-only health/dataset/RBN/policy/clock summaries are isolated to authenticated pyCluster peers and disabled by default
- the System Operator protocol view manages field-level sharing privacy, structured expiring network notices, and direct/reported known-node visibility without a central registry
- upgrade and repair paths protect runtime string catalogs by backing up invalid `strings.toml` files and restoring bundled defaults
- persistent `set/ve7cc` compatibility emits structured CC11 history and live spots for Ham Radio Deluxe while leaving normal user and peer output unchanged

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
- footer `Log In` and `Register` modals for account access, verified account setup, and policy-controlled registration requests
- operate controls appear only after login
- profile editing for normal users

### System Operator Web UI

Operator-facing browser console.

- node presentation and MOTD
- user and access management
- registration state, verification, and unlock controls for local users
- peer and link management
- protocol health and policy drops
- audit and security views

## 🚀 Quick Start

Get the code with SSH:

```bash
cd /usr/src
git clone git@github.com:AI3I/pyCluster.git
cd pyCluster
```

Or with HTTPS:

```bash
cd /usr/src
git clone https://github.com/AI3I/pyCluster.git
cd pyCluster
```

Update an existing checkout:

```bash
git pull --ff-only
```

For production-style installs, start from a clean host. Do not treat pyCluster as a sidecar package to be dropped into an already crowded server with other unrelated products.

Run locally for development:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

pycluster --config ./config/pycluster.toml serve
```

Operator-local overrides can live in `./config/pycluster.local.toml`. When present, pyCluster loads `pycluster.toml` first and then layers `pycluster.local.toml` on top. Keep the tracked base file close to upstream and put host-specific changes in the local override file.

Deploy on a supported Linux host:

```bash
sudo ./deploy/install.sh
sudo ./deploy/doctor.sh
```

For installation support, generate a reviewable report without copying
configuration values, databases, credentials, or user records:

```bash
sudo ./deploy/support-bundle.sh --redacted
```

Running without options shows help. Detailed network state and recent service
journals are opt-in; trusted maintainers can also request an unredacted report,
a consistent SQLite snapshot, or a complete offline lab-import archive. See
[Support Report](docs/operations.md#support-report).

The installer seeds a one-time `SYSOP` account and writes its credentials to
`/root/pycluster-initial-sysop.txt`. The default configuration starts the node,
but its example `N0CALL-1` identity must be replaced under **Node Settings >
General** before the node is treated as production-ready.

Interactive installs now offer to run `deploy/setup-nginx.sh` for you. That flow asks for:

- the public hostname to publish
- an optional separate sysop hostname
- whether nginx should expose ports `80` and `443`
- whether to use Let's Encrypt or self-signed TLS
- the email address required for Let's Encrypt

`deploy/setup-nginx.sh` is the supported out-of-the-gate path when nginx will run on the pyCluster host. It wires nginx to ports `80` and `443` and fails fast if some other non-nginx service already owns those ports. A central reverse proxy on another host is also supported; see [External Reverse Proxy / No Local nginx](docs/installation.md#external-reverse-proxy--no-local-nginx).

For a host-level install, cloning into `/usr/src/pyCluster` is the recommended layout.
The deploy scripts create the `pycluster` system user and group automatically; the installer does not require the operator to create that account first.
The installed runtime tree is placed under `/home/pycluster/pyCluster`.
The intended install target is a clean standalone Linux host dedicated to pyCluster rather than a system already shared with unrelated application stacks.
The same expectation applies to SMTP: configure mail delivery specifically for pyCluster, on the pyCluster host, using the documented settings and local override file, rather than treating pyCluster as a partial add-on to some unrelated mail environment.

Typical deployed layout:

```text
/usr/src/pyCluster                  # administrator managed checkout used for install/upgrade
/home/pycluster/pyCluster/          # live runtime tree
├── config/
│   ├── pycluster.toml              # active node configuration
│   ├── pycluster.local.toml        # optional untracked local override
│   └── strings.toml                # hot-reloadable operator text
├── data/
│   └── pycluster.db                # live SQLite database
├── logs/
│   └── proto/                      # protocol (PCxx) trace logs
└── src/                            # installed application code

/var/log/pycluster/authfail.log     # authentication failure log watched by fail2ban
/root/pycluster-initial-sysop.txt   # bootstrap SYSOP credentials note (needed post-install!)
```

Upgrade an existing deployment:

```bash
git pull --ff-only
sudo ./deploy/upgrade.sh
sudo ./deploy/doctor.sh
```

For git-based upgrades, move site-local changes out of the tracked `config/pycluster.toml` file and into `config/pycluster.local.toml` first. That keeps `git pull --ff-only` clean while preserving local runtime settings.

The supported scripted upgrade path covers `1.0.0` and later. `deploy/upgrade.sh` performs the required cumulative migration chain before services restart:

- `run_upgrade_1_0_1`
  - hashes any legacy plaintext passwords still stored in `user_prefs`
  - seeds `config/strings.toml` if it is missing
- `run_upgrade_1_0_6`
  - moves any embedded outbound peer `password=` values out of DSNs and into the separate peer-password preference path used by current pyCluster

The upgrade path preserves the existing runtime `config/`, `data/`, and `logs/` directories in place. The source tree is synced into the runtime directory with those paths excluded, so local `config/pycluster.toml`, `config/pycluster.local.toml`, SQLite data, imported country data, and operational logs are not overwritten by the repo copy.

`deploy/upgrade.sh`, `deploy/repair.sh`, and `deploy/uninstall.sh` also create timestamped runtime backups under `/root/pycluster-backups/` before making destructive changes to the live tree. On older deployments whose local `deploy/upgrade.sh` predates automatic preflight backups, take a manual backup before pulling or running the upgrade:

```bash
sudo install -d -m 0700 /root/pycluster-backups
sudo tar -C /home/pycluster -czf /root/pycluster-backups/manual-pre-upgrade_$(date -u +%Y%m%dT%H%M%SZ).tar.gz pyCluster/config pyCluster/data pyCluster/logs
```

Default listeners:

- telnet: 0.0.0.0:7300
- sysop web: 127.0.0.1:8080
- public web: 127.0.0.1:8081

Important:

- unless you publish nginx or another reverse proxy in front of them, the two web listeners stay bound to localhost only
- a fresh install is intentionally not public on ports `8080` or `8081`
- `deploy/install.sh` now offers to finish that nginx setup during the install
- `deploy/setup-nginx.sh` is the supported way to claim `80/443` when nginx runs on the pyCluster host
- an external reverse proxy does not require local nginx, but the runtime web listeners and firewall must be configured explicitly
- IPv4-only, IPv6-only, and dual-stack backend listener examples are documented for external proxies
- the systemd services read `/home/pycluster/pyCluster/config/pycluster.toml` and its sibling `pycluster.local.toml`, not the source checkout under `/usr/src/pyCluster`

## 🛠️ Deployment

Production deployment is handled through the checked-in `deploy/` scripts and systemd units.

Validated deployment targets:

- Debian 12 and 13
- Ubuntu 24.04 LTS and 25.10
- Fedora 42 and 43 with SELinux enforcing
- CentOS Stream 9 and 10 with SELinux enforcing
- AlmaLinux 8, 9, and 10 with SELinux enforcing
- Rocky Linux 8, 9, and 10 with SELinux enforcing

Likely install candidates (not yet tested):

- Fedora 44 with SELinux enforcing  (official release April 14, 2026)
- Red Hat 8, 9 and 10 with SELinux enforcing  (presumed working)

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

If you are upgrading from any `1.0.x` release starting at `1.0.0`, use the scripted upgrade path instead of reinstalling. The cumulative migrations are designed to carry older `1.0.x` nodes forward in place.

Installed services:

- `pycluster.service`
- `pyclusterweb.service`
- `pycluster-data-refresh.timer`
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

## 🌍 Country Data

pyCluster supports both `CTY.DAT` and `wpxloc.raw`.

That data is used for:

- DXCC/entity and zone enrichment
- heading and lookup fallbacks
- operational review cues for unusual spot prefixes
- sysop visibility into currently loaded country-data versions

pyCluster does not package `CTY.DAT` or `wpxloc.raw` in the repository. Install, upgrade, repair, and the data refresh timer fetch the active runtime copies from Country Files into `data/`.

Manual refresh:

```bash
python3 ./scripts/update_cty.py --config ./config/pycluster.toml
```

By default this refreshes both `CTY.DAT` and `wpxloc.raw`. Use `--cty-only` if you intentionally want to skip the `wpxloc.raw` update.

Automatic refresh:

- `pycluster-data-refresh.timer`
  - runs shortly after boot and then refreshes both `CTY.DAT` and `wpxloc.raw` every 6 hours

The System Operator web console and telnet `show/configuration` also report dataset load state, path, and version/date when available.

## 🧹 Retention and Cleanup

pyCluster can automatically prune older operational data.

- spots, messages, and bulletins can be retained for configurable day counts
- the System Operator web UI exposes:
  - ability to enable age-based cleanup
  - per-category day values
  - ad-hoc, on-demand cleanup
- scheduled cleanup runs daily through:
  - `pycluster-retention.timer`

## 📚 Documentation

- [Documentation Index](docs/README.md)
- [User Manual](docs/user-manual.md)
- [Administration Manual](docs/administration-manual.md)
- [Installation](docs/installation.md)
- [Migration](docs/migration.md)
- [Configuration](docs/configuration.md)
- [Feature Highlights](docs/feature-highlights.md)
- [Telnet Commands](docs/telnet-commands.md)
- [Telnet Command Reference](docs/telnet-command-reference.md)
- [Command Specification](docs/command-specification.md)
- [System Operator Web](docs/sysop-web.md)
- [Public Web UI](docs/public-web.md)
- [Node Linking](docs/node-linking.md)
- [Security](docs/security.md)
- [Operations](docs/operations.md)
- [Architecture](docs/architecture.md)
- [Roadmap](docs/pycluster-roadmap.md)
- [Changelog](CHANGELOG.md)

## 🙏 Credits

pyCluster was created by John D. Lewis, AI3I, with direction from Joe Reed, N9JR, and help from ChatGPT OpenAI Codex and Anthropic Claude AI.

Special thanks for advice, assistance, consideration and testing:

- Howard Leadmon, WB3FFV
- Eric Tichansky, NO3M

## 🤝 Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## 🕒 Change Log

See [CHANGELOG.md](CHANGELOG.md).
