# Operations

This page covers the day-to-day operator view of a deployed pyCluster node.

## Services

Typical production services:

- `pycluster.service`
- `pyclusterweb.service`
- `pycluster-data-refresh.timer`
  - refreshes both `CTY.DAT` and `wpxloc.raw`
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
systemctl status pycluster.service pyclusterweb.service pycluster-data-refresh.timer
```

## On-Disk Layout

Typical deployed paths:

```text
/usr/src/pyCluster                 # admin-managed checkout used for install/upgrade
/home/pycluster/pyCluster/        # live runtime tree
├── config/
│   ├── pycluster.toml            # active base node config
│   ├── pycluster.local.toml      # optional untracked local override
│   └── strings.toml              # hot-reloadable operator text
├── data/
│   └── pycluster.db              # live SQLite database
├── logs/
│   └── proto/                    # protocol trace logs
└── src/                          # installed application code

/var/log/pycluster/authfail.log   # auth-failure log watched by fail2ban
/root/pycluster-initial-sysop.txt # bootstrap SYSOP credentials note
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
- wpxloc.raw path
- whether the reported `wpxloc.raw` path is explicitly configured or derived from the `cty.dat` sibling path
- loaded dataset version/date shown in the System Operator Console and telnet `show/configuration`
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
- `config/pycluster.local.toml`
- `data/pycluster.db`
- local country-data overrides if you have any

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

This refreshes both `CTY.DAT` and `wpxloc.raw` unless you pass `--cty-only`.

Automatic:

- `pycluster-data-refresh.timer`
  - refreshes both `CTY.DAT` and `wpxloc.raw`

## Security Operations

### fail2ban

pyCluster ships fail2ban filters and jails for auth-failure events.

Relevant repo paths:

- `deploy/fail2ban/filter.d/pycluster-auth-core.conf`
- `deploy/fail2ban/filter.d/pycluster-auth-web.conf`
- `deploy/fail2ban/filter.d/pycluster-auth-scanner.conf`
- `deploy/fail2ban/jail.d/pycluster-core.local`
- `deploy/fail2ban/jail.d/pycluster-web.local`
- `deploy/fail2ban/jail.d/pycluster-scanner.local`

Installed jail names:

- `pycluster-core-auth`
- `pycluster-web-auth`
- `pycluster-telnet-scanner`

Useful checks:

```bash
sudo fail2ban-client status
sudo fail2ban-client status pycluster-core-auth
sudo fail2ban-client status pycluster-web-auth
sudo fail2ban-client status pycluster-telnet-scanner
sudo tail -n 50 /var/log/pycluster/authfail.log
```

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

Install, upgrade, and repair runs refresh the pyCluster fail2ban filters and jails, then restart `fail2ban` when the service is available.

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
- review suspicious spot calls in the System Console spot table

The peer model distinguishes:

- `Dial-out`
- `Accepted`

That is about who initiated the link, not whether traffic is bidirectional.

The web peer table keeps transport state and protocol freshness separate:

- `connected` means a socket is live
- `disconnected` means no socket is live
- `bidirectional`, `receive active`, `transmit active`, `idle`, and `connected quiet` describe recent traffic direction
- protocol stale/degraded/flapping labels describe whether fresh PC protocol traffic has been received inside the configured thresholds

An inbound link can be connected and transmit-active/receive-quiet when the remote node is still connected but has not sent recent protocol traffic. That should not be shown to operators as a broken transport.

When spot ingest sees a callsign that is syntactically plausible but not recognized by the currently loaded prefix data, pyCluster ingests it and logs a `spot call review: ...` line instead of dropping it. The System Console spot table marks those rows with a `Review` badge.


## Country Data Status

When country/prefix data is missing or stale, pyCluster will still ingest plausible spots. The System Operator Console spot review will show an advisory about missing or stale prefix data rather than treating every unknown prefix as a suspicious callsign.
