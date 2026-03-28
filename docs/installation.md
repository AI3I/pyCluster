# Installation

## Requirements

- Linux
- Python 3.11+
- systemd for the supported deployment path

Recommended:

- reverse proxy for public exposure
- fast local storage for SQLite
- fail2ban

## Validated Platforms

The deploy scripts have been validated on:

- Debian 12 and 13
- Ubuntu 24.04 LTS and 25.10
- Fedora 42 and 43 with SELinux enforcing
- CentOS Stream 9 and 10 with SELinux enforcing
- AlmaLinux 8, 9, and 10 with SELinux enforcing
- Rocky Linux 8, 9, and 10 with SELinux enforcing

Not yet directly validated:

- Raspberry Pi OS / Raspbian
- Red Hat Enterprise Linux
- Oracle Linux

Support guidance:

- RHEL-family support is strongly indicated by the validated CentOS Stream, AlmaLinux, and Rocky Linux paths
- Red Hat Enterprise Linux should be described as expected to work on 9/10-class systems, but not yet directly tested
- Oracle Linux is likely to work as an EL-family target, but it should stay in the unvalidated bucket until it is tested directly
- Raspberry Pi OS / Raspbian is plausible on 64-bit Debian-family images, but should not be claimed as tested yet

Do not target older distro baselines for the supported deployment path:

- Debian 11
- Ubuntu 22.04 LTS
- CentOS 7
- Red Hat Enterprise Linux 7 and below
- Oracle Linux 7 and below

Reason:

- pyCluster requires Python 3.11+
- older distro baselines are too old for the current dependency/runtime requirements

## Hardware and Resource Guidance

Minimum practical node:

- 1 vCPU
- 1 GB RAM
- 10 GB disk

Recommended small production node:

- 2 vCPU
- 2 GB RAM
- 20 GB SSD-backed disk

Additional notes:

- 1 GB RAM works, but leaves less headroom during package operations and service restarts
- EL-family hosts with 1 GB RAM may require temporary swap during package installation; the deploy scripts handle that automatically
- if you plan to run reverse proxy, TLS, fail2ban, and longer spot retention on the same host, prefer 2 GB RAM or better

## Local Development Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Run the core service:

```bash
pycluster --config ./config/pycluster.toml serve
```

## Production Install

From the repo root:

```bash
sudo ./deploy/install.sh
sudo ./deploy/doctor.sh
```

This installs:

- application tree under `/home/pycluster/pyCluster`
- `pycluster.service`
- `pyclusterweb.service`
- `pycluster-cty-refresh.timer`
- `pycluster-retention.timer`
- fail2ban filters and jails for pyCluster auth failures
- logrotate policy for `/var/log/pycluster/authfail.log`
- an initial `SYSOP` account bootstrap note at `/root/pycluster-initial-sysop.txt`

During install and repair, pyCluster now prints the bootstrap `SYSOP` credentials prominently in the terminal, saves the same note to `/root/pycluster-initial-sysop.txt`, and pauses interactive installs until the operator explicitly acknowledges that the credentials were reviewed.

## Upgrade

```bash
sudo ./deploy/upgrade.sh
sudo ./deploy/doctor.sh
```

For the `1.0.0` to `1.0.1` upgrade path, `deploy/upgrade.sh` now also:

- hashes any legacy plaintext passwords still stored in the local SQLite `user_prefs` table
- seeds `config/strings.toml` if it is missing
- preserves the existing `config/pycluster.toml`, data, and logs in place

## Repair

```bash
sudo ./deploy/repair.sh
```

## Uninstall

Keep config and data:

```bash
sudo ./deploy/uninstall.sh
```

## DXSpider Migration

After pyCluster is installed, the first migration pass from DXSpider is available through:

```bash
sudo ./deploy/migrate.sh --from-dxspider /spider --dry-run
sudo ./deploy/migrate.sh --from-dxspider /spider
```

See [Migration](migration.md) for details and current scope.

Current migration behavior also includes:

- simple outbound DXSpider peer import from `connect/*`
- exact `badip.local` IP entries exported to `config/fail2ban-badip.local`
- reconciliation of imported exact IPs into the active pyCluster fail2ban block set
- unsupported connect scripts and CIDR-style `badip.local` entries are reported, not guessed

Remove config and data too:

```bash
sudo KEEP_CONFIG=0 KEEP_DATA=0 ./deploy/uninstall.sh
```

## First Checks

```bash
systemctl status pycluster.service pyclusterweb.service
sudo ./deploy/doctor.sh
```

If the first install created the bootstrap account successfully, you should also see:

```bash
sudo ls -l /root/pycluster-initial-sysop.txt
```

That file contains the one-time generated `SYSOP` password for first web-based operator login.

If the install is interactive, the deploy script now stops and requires `READ` confirmation before it continues past the bootstrap credential notice.

## Retention and Cleanup

pyCluster supports scheduled age-based cleanup for:

- spots
- messages
- bulletins

The scheduler is installed as:

- `pycluster-retention.timer`

You can manage retention from the System Operator web UI or run cleanup manually through the UI action.

## Log Rotation

The deploy scripts install logrotate coverage for:

- `/var/log/pycluster/authfail.log`

That policy rotates weekly, keeps compressed history, and prevents the auth-failure log from growing without bound on long-running nodes.

## Default Ports

- telnet: `7300`
- sysop web: `8080`
- public web: `8081`

## Optional Dependencies

Serial/KISS support:

```bash
pip install pyserial
```
