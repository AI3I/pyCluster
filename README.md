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
- SQLite persistence, CTY and wpxloc refresh tooling, and fail2ban integration
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
- maintains local `CTY.DAT` and `wpxloc.raw` data with optional automatic refresh from Country Files

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
- bundled and refreshable CTY data instead of relying on stale host copies
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

Current release: `1.0.7`

Recent highlights in `1.0.7`:

- user deletion now removes the full local account footprint instead of leaving stale prefs and records behind
- new-user approval again creates limited-access users by default rather than over-permissive accounts
- telnet `show/sun`, `show/moon`, `show/muf`, `show/wcy`, and `show/wwv` behavior was tightened and regression-covered
- public web spot toasts no longer cover the sidebar, and operators can now hide the sidebar entirely
- historical taxonomy/comment tags were restored, and the SysOp web UI can now edit taxonomy directly
- fresh deployments now ship with all authentication toggles off by default until a sysop enables them

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
- footer `Log In` and `Register` modals for account access and requests
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

Interactive installs now offer to run `deploy/setup-nginx.sh` for you. That flow asks for:

- the public hostname to publish
- an optional separate sysop hostname
- whether nginx should expose ports `80` and `443`
- whether to use Let's Encrypt or self-signed TLS
- the email address required for Let's Encrypt

For a host-level install, cloning into `/usr/src/pyCluster` is the recommended layout.
The deploy scripts create the `pycluster` system user and group automatically; the installer does not require the operator to create that account first.
The installed runtime tree is placed under `/home/pycluster/pyCluster`.

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

For upgrades from any release below `1.0.6`, `deploy/upgrade.sh` performs the required cumulative migration chain before services restart:

- `run_upgrade_1_0_1`
  - hashes any legacy plaintext passwords still stored in `user_prefs`
  - seeds `config/strings.toml` if it is missing
- `run_upgrade_1_0_6`
  - moves any embedded outbound peer `password=` values out of DSNs and into the separate peer-password preference path used by current pyCluster

The upgrade path still preserves the existing `config/pycluster.toml`, local overrides, data, and logs in place.

Default listeners:

- telnet: 0.0.0.0:7300
- sysop web: 127.0.0.1:8080
- public web: 127.0.0.1:8081

Important:

- unless you publish nginx or another reverse proxy in front of them, the two web listeners stay bound to localhost only
- a fresh install is intentionally not public on ports `8080` or `8081`
- `deploy/install.sh` now offers to finish that nginx setup during the install

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

## 🌍 Country Data

pyCluster supports both `CTY.DAT` and `wpxloc.raw`.

That data is used for:

- DXCC/entity and zone enrichment
- heading and lookup fallbacks
- operational review cues for unusual spot prefixes
- sysop visibility into currently loaded country-data versions

pyCluster ships with bundled country-data fixtures, and install/upgrade can perform a best-effort refresh from Country Files.

Manual refresh:

```bash
python3 ./scripts/update_cty.py --config ./config/pycluster.toml
```

By default this refreshes both `CTY.DAT` and `wpxloc.raw`. Use `--cty-only` if you intentionally want to skip the `wpxloc.raw` update.

Automatic refresh:

- `pycluster-cty-refresh.timer`
  - refreshes both `CTY.DAT` and `wpxloc.raw`

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
