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

If you need machine-local settings, create `./config/pycluster.local.toml`. pyCluster automatically loads it after the base `pycluster.toml`, so local edits do not need to live in the tracked repo file.

## Production Install

From the repo root:

```bash
cd /usr/src
git clone git@github.com:AI3I/pyCluster.git
cd pyCluster
sudo ./deploy/install.sh
sudo ./deploy/doctor.sh
```

Recommended layout for a host-level install:

- checkout under `/usr/src/pyCluster`
- let `deploy/install.sh` create the `pycluster` system user and group automatically

There is no separate pre-install account creation step for the operator to perform.

Interactive installs now also offer to run nginx setup before finishing. That prompt asks for:

- the public hostname for the user web UI
- an optional separate hostname for the sysop web UI
- whether nginx should publish ports `80` and `443`
- whether to use Let's Encrypt or self-signed TLS
- the email address required by Let's Encrypt

If you skip nginx setup, pyCluster still installs and starts cleanly, but:

- the sysop web UI stays on `127.0.0.1:8080`
- the public web UI stays on `127.0.0.1:8081`
- those listeners are local-only until you publish them with nginx or another reverse proxy

This installs:

- application tree under `/home/pycluster/pyCluster`
- `pycluster.service`
- `pyclusterweb.service`
- `pycluster-cty-refresh.timer`
  - keeps `CTY.DAT` and `wpxloc.raw` current from the Country Files refresh path
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

For upgrades from any release below `1.0.6`, `deploy/upgrade.sh` runs the cumulative migration chain required by older installs before services restart. The current chain includes:

- `run_upgrade_1_0_1`
  - hash legacy plaintext user passwords
  - seed `config/strings.toml` when it is missing
- `run_upgrade_1_0_6`
  - move any embedded outbound peer `password=` values out of DSNs and into the separate peer-password preference path used by current pyCluster

Recommended before future upgrades:

- keep `config/pycluster.toml` close to upstream defaults
- move host-specific changes into `config/pycluster.local.toml`
- leave `config/pycluster.local.toml` untracked so `git pull --ff-only` stays clean

That local override file is also the right place for host-specific secrets such as QRZ credentials, SMTP credentials, and any node identity or listener changes that should survive repo updates.

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

## MFA Recovery

Normal operator recovery paths:

- System Console: use `Reset MFA` on the user record
- Telnet sysop command: `sysop/clearmfa <call>`

Both paths:

- force `mfa_email_otp=off` for the principal/base callsign
- clear outstanding OTP challenges for that callsign and its SSIDs
- leave the password unchanged
- write an audit record

If every sysop is locked out, recover locally on the host:

```bash
cd /home/pycluster/pyCluster
sqlite3 data/pycluster.db "
DELETE FROM mfa_challenges;
DELETE FROM user_prefs WHERE call='AI3I' AND pref_key='mfa_email_otp';
INSERT INTO user_prefs(call,pref_key,pref_value,updated_epoch)
VALUES('AI3I','mfa_email_otp','off',strftime('%s','now'))
ON CONFLICT(call,pref_key) DO UPDATE SET
  pref_value=excluded.pref_value,
  updated_epoch=excluded.updated_epoch;
"
systemctl restart pycluster.service pyclusterweb.service
```

Replace `AI3I` with the principal/base sysop callsign you are recovering. If needed, also reset the password through the existing bootstrap or direct SQLite recovery path.

## Registration Recovery

Ordinary-user registration now tracks:

- registration state
- verified email status
- grace logins remaining before lockout

Normal operator recovery paths:

- System Console user editor:
  - `Send Verification`
  - `Mark Verified`
  - `Unlock Account`

If an account is locked or stuck pending verification and you need to recover it locally on the host:

```bash
cd /home/pycluster/pyCluster
sqlite3 data/pycluster.db "
DELETE FROM mfa_challenges;
DELETE FROM user_prefs WHERE call='AI3I' AND pref_key IN ('email_verified_epoch','registration_state','grace_logins_remaining');
INSERT INTO user_prefs(call,pref_key,pref_value,updated_epoch)
VALUES('AI3I','registration_state','pending',strftime('%s','now'))
ON CONFLICT(call,pref_key) DO UPDATE SET
  pref_value=excluded.pref_value,
  updated_epoch=excluded.updated_epoch;
INSERT INTO user_prefs(call,pref_key,pref_value,updated_epoch)
VALUES('AI3I','grace_logins_remaining','5',strftime('%s','now'))
ON CONFLICT(call,pref_key) DO UPDATE SET
  pref_value=excluded.pref_value,
  updated_epoch=excluded.updated_epoch;
"
systemctl restart pycluster.service pyclusterweb.service
```

Replace `AI3I` with the principal/base callsign and adjust `5` to match your configured `initial_grace_logins` policy if needed.

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

Default bind behavior:

- telnet listens publicly unless you change `telnet.host`
- the sysop web service listens on `127.0.0.1:8080`
- the public web service listens on `127.0.0.1:8081`

That localhost binding is intentional. A fresh install is not meant to expose the web UI directly until you finish reverse-proxy setup.

## Reverse Proxy Setup

The supported reverse-proxy path is:

```bash
sudo ./deploy/setup-nginx.sh
```

You can also let `deploy/install.sh` call that for you interactively during first install.

Typical nginx setup choices:

- `--public-host cluster.example.net`
- optional `--sysop-host sysop.example.net`
- `--tls-mode none`
- `--tls-mode self-signed`
- `--tls-mode letsencrypt --email admin@example.net`

## Optional Dependencies

Serial/KISS support:

```bash
pip install pyserial
```
