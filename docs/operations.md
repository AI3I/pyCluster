# Operations

This page covers the day-to-day operator view of a deployed pyCluster node.

## Services

Typical production services:

- `pycluster.service`
- `pyclusterweb.service`
- `pycluster-cty-refresh.timer`
- `pycluster-retention.timer`

Validated operational environments so far:

- Debian 12 and 13
- Ubuntu 24.04 LTS and 25.10
- Fedora 42 and 43 with SELinux enforcing
- CentOS Stream 9 and 10 with SELinux enforcing
- AlmaLinux 8, 9, and 10 with SELinux enforcing
- Rocky Linux 8, 9, and 10 with SELinux enforcing

Operational stance:

- these results are strong enough to describe pyCluster as working on modern Debian-family and EL-family systemd hosts
- Red Hat Enterprise Linux is expected to follow the same EL-family path, but has not yet been tested on a subscription-backed host
- Oracle Linux remains likely but unverified
- older distro baselines are out of scope for the supported deployment path because pyCluster requires Python 3.11+
- specifically, do not target Debian 11, Ubuntu 22.04 LTS, or EL 7-era systems and older

Check them:

```bash
systemctl status pycluster.service pyclusterweb.service pycluster-cty-refresh.timer
```

## Deploy Scripts

Supported operational scripts:

- `deploy/install.sh`
- `deploy/upgrade.sh`
- `deploy/repair.sh`
- `deploy/uninstall.sh`
- `deploy/doctor.sh`

## Doctor Output

`deploy/doctor.sh` reports:

- service state
- config path
- SQLite database path
- CTY file path
- CTY timer state
- fail2ban service state
- SELinux state, when available
- SYSOP bootstrap note presence
- public branding response

## Retention Operations

pyCluster supports automatic age-based cleanup for:

- spots
- messages
- bulletins

Operationally, that means:

- retention can be enabled or disabled from the System Operator web UI
- the UI stores separate day counts for spots, messages, and bulletins
- cleanup can be run immediately from the UI with `Run Cleanup Now`
- scheduled cleanup runs daily through:
  - `pycluster-retention.timer`

The node settings UI also reports the last cleanup run and the last removal counts.

## Backups

At minimum, back up:

- `config/pycluster.toml`
- `data/pycluster.db`
- local CTY overrides if you have any

## Resource Planning

For a small single-node deployment, plan around:

- minimum: 1 vCPU, 1 GB RAM, 10 GB disk
- recommended: 2 vCPU, 2 GB RAM, 20 GB SSD-backed disk

Operational observations from validation:

- Debian and Ubuntu are comfortable on small hosts
- Fedora and EL-family hosts with SELinux enforcing also work, but 1 GB RPM-based hosts may need temporary swap during package installation
- the deploy scripts now handle that temporary swap automatically on low-memory EL-family systems

## CTY Refresh

Manual:

```bash
python3 ./scripts/update_cty.py --config ./config/pycluster.toml
```

Automatic:

- `pycluster-cty-refresh.timer`

## Security Operations

### fail2ban

pyCluster ships fail2ban filters and jails for auth-failure events.

Relevant repo paths:

- `deploy/fail2ban/filter.d/pycluster-auth-core.conf`
- `deploy/fail2ban/filter.d/pycluster-auth-web.conf`
- `deploy/fail2ban/jail.d/pycluster-core.local`
- `deploy/fail2ban/jail.d/pycluster-web.local`

Legacy migration integration:

- exact IP entries imported from DXSpider `badip.local` are written to:
  - `config/fail2ban-badip.local`
- deploy and migration runs reconcile that file into the active fail2ban-managed pyCluster block set

### Log Rotation

pyCluster deploys logrotate coverage for:

- `/var/log/pycluster/authfail.log`

That keeps the auth-failure log from growing without bound on long-running systems.

### Sysop Security View

The System Operator web console includes:

- recent auth failures
- current bans

## Telnet and Web Health

Core health:

```bash
curl -fsS http://127.0.0.1:8080/health
```

Public web health:

```bash
curl -fsS http://127.0.0.1:8081/health
```

## Peer Operations

Common operator tasks:

- save a peer definition
- connect a dial-out peer
- disconnect a peer
- inspect `show/links`
- inspect protocol history and policy drops

The peer model distinguishes:

- `Dial-out`
- `Accepted`

That is about who initiated the link, not whether traffic is bidirectional.
