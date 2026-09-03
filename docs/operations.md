# Operations

This page covers the day-to-day operator view of a deployed pyCluster node.

## Test Suites

The default regression suite does not open TCP listeners and therefore does not
trigger desktop or execution-environment firewall authorization prompts:

```bash
pytest -q
```

Real TCP login and transport integration tests are retained separately. They
bind only to loopback addresses and never modify firewall rules, but some host
environments still request listener authorization when they run:

```bash
pytest -q -m socket_listener
```

Collection marks any test that awaits a server `start()` call, opens a socket,
or creates a datagram endpoint, regardless of the local variable name. Autouse
runtime guards reject any unmarked test before a socket bind, connect, datagram
send, or DNS lookup can reach the host. Host address discovery and Fail2Ban
status are replaced with deterministic fixtures. This keeps new or renamed
network tests and host-service probes from silently reintroducing desktop
firewall prompts into the default regression command.

## Services

Typical production services:

- `pycluster.service`
- `pyclusterweb.service`
- `pycluster-data-refresh.timer`
  - runs shortly after boot and then checks for updated `CTY.DAT`, `wpxloc.raw`, and satellite Keps every 6 hours
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
│   ├── strings.toml              # hot-reloadable operator text
│   └── strings.defaults.toml     # managed baseline for upgrade merges
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

The System Operator console upgrade button queues a request under the live runtime tree, then the `pycluster-upgrade.path` unit runs the worker from `/usr/src/pyCluster`. The worker fetches release tags, checks out the newest semantic-version tag above the running version, and runs `deploy/upgrade.sh`, which syncs the updated tree into `/home/pycluster/pyCluster`. The console distinguishes the latest remote release from the source checkout's cached tag and shows the idempotent legacy-state migration separately from the release target.

The version check runs read-only Git commands against the recorded source checkout as the web-service account. Failures include Git's actual diagnostic in **Remote Check**. The console disables `Run Upgrade` when that checkout has local changes or when its source root, `.git` directory, or upgrade script is not root-owned and protected from group/world writes; the root worker checks again before using a forced release checkout. `/usr/src/pyCluster` remains the preferred source layout. A nonstandard source location can be recorded, but it must meet the same ownership boundary before web upgrades are allowed. Manual root upgrades remain available from other checkouts.

Install, upgrade, repair, and uninstall operations preserve the runtime `config/`, `data/`, and `logs/` directories and create timestamped archives under `/root/pycluster-backups/` before making destructive changes. Upgrade and repair three-way merge bundled text changes into `config/strings.toml`, preserving operator-edited values and extra keys against the managed `config/strings.defaults.toml` baseline. Do not edit the baseline file. The scripts stop live writers, active maintenance jobs, and the upgrade watcher before archiving so SQLite and its WAL are captured consistently; a failed preflight or maintenance run restores services that were active before shutdown. It does not automatically replace the runtime database from backup, because a blind rollback could discard valid writes made around the maintenance boundary. Successful install, upgrade, and repair runs require every configured telnet port plus the System Operator and enabled public HTTP health endpoints to answer before reporting completion. If verification fails, recent systemd state and journals are printed and the preflight archive remains available for deliberate recovery. If an older installed checkout does not yet include automatic preflight backups, create one manually first:

```bash
sudo install -d -m 0700 /root/pycluster-backups
sudo tar -C /home/pycluster -czf /root/pycluster-backups/manual-pre-upgrade_$(date -u +%Y%m%dT%H%M%SZ).tar.gz pyCluster/config pyCluster/data pyCluster/logs
```

If a manual backup races with actively written logs, stop `pycluster.service` and `pyclusterweb.service`, repeat the backup, and then run `deploy/upgrade.sh`; the upgrade script restarts both services.

Install, upgrade, and repair runs keep the current `pycluster-data-refresh.*` units and remove the old CTY-only `pycluster-cty-refresh.*` units from earlier deployments. Non-dry-run DXSpider migrations also create a `migration-preflight` backup before writing to the live store.

Install, upgrade, and repair also apply the versioned PY default policy. An installation without the policy marker is enabled once with every read-only PY sharing control active. The marker is then persisted in `pycluster.local.toml`, so later System Operator opt-outs are not reverted. Node call, locator, QTH, contact, services, and software version come from existing node/runtime settings; a valid non-project Website URL is inherited as the PY public URL only when public web is enabled.

## Doctor Output

`deploy/doctor.sh` reports:

- service state
- config path
- SQLite database path
- CTY file path
- data refresh timer state
- retention timer and on-demand upgrade watcher state
- registration-reminder timer state
- wpxloc.raw path
- whether the reported `wpxloc.raw` path is explicitly configured or derived from the `cty.dat` sibling path
- loaded dataset version/date shown in the System Operator Console and telnet `show/configuration`
- fail2ban service state
- SELinux state, when available
- SYSOP bootstrap note presence
- public branding response
- effective base-plus-local configuration paths and public web port
- effective telnet, System Operator web, and public web listener bindings
- verified local reachability for every configured telnet port and each enabled HTTP `/health` endpoint
- effective PY protocol state and the number of enabled sharing controls; a disabled node is directed to Node Settings > pyCluster Protocol

Configured bindings describe intent; the separate runtime-health line confirms that processes are actually accepting connections. `doctor.sh` exits nonzero if required account, configuration, database, service, listener, or public API checks fail, making it suitable for scripted post-upgrade validation.

The daily `pycluster-registration-reminders.timer` emails the applicant when a pending request reaches 1, 4, 7, 10, and 14 days old. Delivered stages are recorded in SQLite. If the timer was offline, the next run sends only the latest due reminder rather than every missed message; automatic reminders stop after day 14. Invalid/missing applicant addresses and SMTP delivery failures do not advance the stored stage, allowing correction and retry.

## Support Report

When an installation is incomplete, modified, or difficult to identify, collect
a text-only support report:

```bash
cd /usr/src/pyCluster
sudo ./deploy/support-bundle.sh --redacted
```

Running the collector without options displays its help and does not create a
report. Every collection must explicitly choose `--redacted` or `--unredacted`.
The report is written under `/tmp` by default with mode `0600`. It inventories
the OS, kernel, CPU, memory, storage, virtualization/container indicators,
runtime tools, service units, listeners, SELinux, nginx, fail2ban, source and
runtime versions, immutable-tree differences, ownership anomalies, possible
duplicate installations, and the built-in doctor results. It ends with a
plain-language installation assessment.

Redacted mode masks configuration values, network addresses, and MAC addresses
where practical. Unredacted mode includes host/network identifiers and
non-secret configuration values for private troubleshooting. Passwords,
tokens, DSNs, API keys, and similar credentials remain redacted in both modes.
The normal report does not copy bootstrap credentials, mail, private messages,
or user records.

The protocol section reports the configured, detected, and effective IPv4/IPv6
addresses used by PC frames, runs a local PC92 `localhost` substitution probe,
lists saved peer endpoints without credentials, checks SQLite integrity and row
counts, and summarizes recent PC18/PC61/PC92/PC93/PY traffic. This is the first
report to request for private-address or `localhost` reports such as issue #193.

Detailed addresses, routes, DNS state, policy routing, network namespaces,
established sockets, and nftables/iptables/UFW/firewalld state require
`--include-network`. Recent service journals remain separately opt-in:

```bash
sudo ./deploy/support-bundle.sh --redacted --include-network --include-journal
```

Review the report before attaching it to a public issue. Hostnames, listener
addresses, process metadata, and explicitly included logs can identify a node.

For trusted private troubleshooting, request an unredacted report. It still
removes credentials:

```bash
sudo ./deploy/support-bundle.sh --unredacted --include-network --include-journal
```

To export only a transactionally consistent copy of the live SQLite store:

```bash
sudo ./deploy/support-bundle.sh --unredacted --include-database
```

The database snapshot is written beside the report with mode `0600`. It contains
accounts, preferences, mail, and operational history and must never be attached
to a public issue.

For an offline reproduction environment, `--include-instance` creates a
restricted `tar.gz` containing the deployed runtime (excluding virtual
environments and live SQLite sidecars), complete config/data/logs, a consistent
database snapshot, pyCluster-specific systemd/nginx/fail2ban/logrotate files,
SELinux configuration when present, ACL metadata, and a Git source bundle plus
working-tree patch. A SHA-256 file is written beside it:

```bash
sudo ./deploy/support-bundle.sh --unredacted --include-network \
  --include-journal --include-instance
```

Treat this archive as a full copy of the node. Transfer it only over an
authenticated encrypted channel. Extract it into a new empty directory on an
isolated test host, inspect `MANIFEST.txt`, and point test tooling at
`database/pycluster.sqlite3` and `runtime/config`; do not extract it over
`/home/pycluster/pyCluster` or use it to overwrite a live node. Historical
protocol logs created before the credential-safe connection-trace fix may
contain effective peer DSNs, which is another reason the archive must remain
private and peer credentials should be rotated if such an archive is exposed.

Successful install, upgrade, and repair runs now write
`data/deployment-state.toml` with the completed action, version, source commit,
and source-tree state. The collector treats a missing receipt as a warning, not
proof of manual copying, because older supported installs did not create one.

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

## Runtime Data Refresh

Manual:

```bash
python3 ./scripts/update_cty.py --config ./config/pycluster.toml
```

This refreshes `CTY.DAT`, `wpxloc.raw`, and satellite Keps unless you pass `--country-only` or `--cty-only`. Every download is validated before atomic replacement, and existing files remain in place when validation or download fails.

Automatic:

- `pycluster-data-refresh.timer`
  - runs shortly after boot and then checks for updated country and satellite data every 6 hours

## Security Operations

### fail2ban

pyCluster ships fail2ban filters and jails for auth-failure events.

Relevant repo paths:

- `deploy/fail2ban/filter.d/pycluster-auth-core.conf`
- `deploy/fail2ban/filter.d/pycluster-auth-web.conf`
- `deploy/fail2ban/filter.d/pycluster-auth-telnet.conf`
- `deploy/fail2ban/filter.d/pycluster-auth-scanner.conf`
- `deploy/fail2ban/action.d/pycluster-lock-account.conf`
- `deploy/fail2ban/jail.d/pycluster-core.local`
- `deploy/fail2ban/jail.d/pycluster-web.local`
- `deploy/fail2ban/jail.d/pycluster-telnet.local`
- `deploy/fail2ban/jail.d/pycluster-scanner.local`

Installed jail names:

- `pycluster-core-auth`
- `pycluster-telnet-auth`
- `pycluster-web-auth`
- `pycluster-telnet-scanner`

The SysOp-web and telnet jails each use a five-failure threshold and affect only their respective service ports. Password failures for an account with verified email and configured recovery mail use pyCluster's durable account lock instead of an IP ban, preserving immediate public reset and telnet reconnection. Malformed, unverified, and mail-unrecoverable attempts remain bannable. Password and MFA recovery are scoped to the exact callsign-SSID and require its matching verified email address plus a purpose-bound code. MFA recovery preserves passwords, unrelated locks, and node-wide MFA policy. The aggressive scanner jail matches malformed callsigns only; normal registration requests and policy-denied logins do not count as scanner traffic. Installation does not alter the host's SSH jail policy.

Useful checks:

```bash
sudo fail2ban-client status
sudo fail2ban-client status pycluster-core-auth
sudo fail2ban-client status pycluster-telnet-auth
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

Peer protocol traces under `/home/pycluster/pyCluster/logs/proto/` are pruned separately by `pycluster-retention.timer`. The default is 14 days and the System Operator can shorten it with **Keep Protocol Logs For (days)** under Node Settings > Maintenance. **Protocol Log Detail** can retain full frames, retain connection/error events without high-volume RX/TX payloads, or disable new protocol trace entries.

### Sysop Security View

The System Operator web console includes:

- recent auth failures
- current bans

Install, upgrade, and repair runs refresh the pyCluster fail2ban filters, optional actions, and jails, then restart `fail2ban` when the service is available.

Upgrade and repair also remove obsolete rows whose `source_node` is `RBN`. New RBN reports are live-only and do not enter SQLite. This cleanup makes deleted pages reusable; operators who need the database file itself to shrink can stop both services, make a backup, and run `sqlite3 data/pycluster.db 'VACUUM;'` with sufficient free disk space.

By default fail2ban bans IP addresses only. To also lock the user account named in a structured auth-failure line, add the optional `pycluster-lock-account` action to a local jail override and set `pycluster_db` to the active SQLite database. The action calls `scripts/lock_user_account.py`, which can also be run manually on the host for emergency lock/unlock work.

## Telnet and Web Health

Confirm the effective configured listeners and the sockets actually opened by systemd services:

```bash
sudo /usr/src/pyCluster/deploy/doctor.sh
sudo ss -lntp | grep -E ':(7300|7373|8000|8080|8081)\b'
sudo journalctl -u pycluster.service -u pyclusterweb.service -n 100 --no-pager
```

The installed services load `/home/pycluster/pyCluster/config/pycluster.toml` and its sibling `pycluster.local.toml`. The `/usr/src/pyCluster` checkout supplies upgrade code and documentation; editing its configuration does not alter the running installation.

Core health:

```bash
curl -fsS http://127.0.0.1:8080/health
```

Public web health:

```bash
curl -fsS http://127.0.0.1:8081/health
```

For a listener bound to a specific LAN address instead of loopback, substitute that address in the health URL; enclose IPv6 literals in brackets, for example `http://[2001:db8:100::20]:8081/health`. A socket shown on `0.0.0.0` or `[::]` but unreachable from another machine points to the host firewall, VM networking, VLAN policy, routing, or an upstream ACL rather than nginx.

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

The web surfaces keep transport state and protocol freshness separate:

- Peers and Links uses `connected` or `disconnected` for the live socket and shows direction, retry state, last RX/TX time, and traffic counts.
- The peer modal carries detailed transport, error, queue, and protocol information.
- Protocol Health owns stale/degraded/flapping labels based on received PC protocol freshness and configured thresholds.

A live socket and fresh inbound protocol traffic are related but distinct. Diagnose a disconnected row as transport/DNS/listener failure first; diagnose a connected but stale row on Protocol Health as a protocol-activity problem.

When spot ingest sees a callsign that is syntactically plausible but not recognized by the currently loaded prefix data, pyCluster ingests it and logs a `spot call review: ...` line instead of dropping it. The System Console spot table marks those rows with a `Review` badge.


## Country Data Status

When country/prefix data is missing or stale, pyCluster will still ingest plausible spots. The System Operator Console spot review will show an advisory about missing or stale prefix data rather than treating every unknown prefix as a suspicious callsign.
