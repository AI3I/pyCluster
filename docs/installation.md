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
- use a clean standalone Linux host dedicated to pyCluster rather than trying to combine it with unrelated products or pre-existing service bundles

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

`deploy/setup-nginx.sh` is the supported out-of-the-gate path when nginx will run on the pyCluster host. A separately managed reverse proxy on another trusted host is also supported and does not require nginx on the pyCluster VM. See [External Reverse Proxy / No Local nginx](#external-reverse-proxy--no-local-nginx).

This installs:

- application tree under `/home/pycluster/pyCluster`
- `pycluster.service`
- `pyclusterweb.service`
- `pycluster-data-refresh.timer`
  - runs shortly after boot and then every 6 hours for updated `CTY.DAT`, `wpxloc.raw`, and amateur-satellite Keps
- `pycluster-retention.timer`
- fail2ban filters and jails for pyCluster auth failures
- logrotate policy for `/var/log/pycluster/authfail.log`
- an initial `SYSOP` account bootstrap note at `/root/pycluster-initial-sysop.txt`

During install and repair, pyCluster now prints the bootstrap `SYSOP` credentials prominently in the terminal, saves the same note to `/root/pycluster-initial-sysop.txt`, and pauses interactive installs until the operator explicitly acknowledges that the credentials were reviewed.

The default runtime `pycluster.toml` is sufficient to start both services and
complete the first `SYSOP` login. It intentionally begins with example node
identity values such as `N0CALL-1`. After that first login, open **Node Settings
> General**, replace the example callsign, owner, QTH, locator, support contact,
and public URL as applicable, then save. The System Operator console writes
host-specific values to `pycluster.local.toml`, which is layered over the base
file and preserved across upgrades. An installation is operational before this
step, but it is not ready to advertise or peer as a real node while the example
identity remains active.

## Upgrade

```bash
sudo ./deploy/upgrade.sh
sudo ./deploy/doctor.sh
```

The supported scripted upgrade path covers `1.0.0` and later. `deploy/upgrade.sh` runs the cumulative migration chain required by older installs before services restart. The current chain includes:

- `run_upgrade_1_0_1`
  - hash legacy plaintext user passwords
- `run_upgrade_1_0_6`
  - move any embedded outbound peer `password=` values out of DSNs and into the separate peer-password preference path used by current pyCluster

The upgrade path preserves the existing runtime `config/`, `data/`, and `logs/` directories in place. Upgrade and repair first record which services are active, stop live writers, and take a consistent preflight backup before synchronizing code or running migrations. A failed maintenance run attempts to restore the previously active services. Successful runs restart through systemd's normal graceful stop/start behavior. The source tree is synced with runtime paths excluded, so local `config/pycluster.toml`, `config/pycluster.local.toml`, SQLite data, imported country data, and operational logs are not overwritten by the repo copy.

Current install, upgrade, and repair runs install `pycluster-data-refresh.timer` and remove the older CTY-only `pycluster-cty-refresh.*` units if they exist from an earlier deployment.

`deploy/upgrade.sh`, `deploy/repair.sh`, and `deploy/uninstall.sh` also create timestamped runtime backups under `/root/pycluster-backups/` before making destructive changes to the live tree. On older deployments whose local `deploy/upgrade.sh` predates automatic preflight backups, take a manual backup before pulling or running the upgrade:

```bash
sudo install -d -m 0700 /root/pycluster-backups
sudo tar -C /home/pycluster -czf /root/pycluster-backups/manual-pre-upgrade_$(date -u +%Y%m%dT%H%M%SZ).tar.gz pyCluster/config pyCluster/data pyCluster/logs
```

The System Operator console upgrade action uses the same path indirectly: it writes a request in `/home/pycluster/pyCluster/data`, the `pycluster-upgrade.path` unit starts a worker from `/usr/src/pyCluster`, and that worker advances the source checkout before running `deploy/upgrade.sh`.

Recommended before future upgrades:

- keep `config/pycluster.toml` close to upstream defaults
- move host-specific changes into `config/pycluster.local.toml`
- leave `config/pycluster.local.toml` untracked so `git pull --ff-only` stays clean

That local override file is also the right place for host-specific secrets such as QRZ credentials, SMTP credentials, and any node identity or listener changes that should survive repo updates.

SMTP should also be treated as part of the pyCluster host configuration. The supported path is to configure the mail settings pyCluster needs for its own delivery behavior, not to assume pyCluster is meant to be bolted onto some unrelated pre-existing mail stack without dedicated setup.

## Repair

```bash
sudo ./deploy/repair.sh
```

## Uninstall

Keep config and data:

```bash
sudo ./deploy/uninstall.sh
```

Uninstall disables and removes the on-demand upgrade path/service along with the core, web, refresh, and retention units. The default keep-data behavior does not leave an active watcher behind.

## DXSpider Migration

After pyCluster is installed, the first migration pass from DXSpider is available through:

```bash
sudo ./deploy/migrate.sh --from-dxspider /spider --dry-run
sudo ./deploy/migrate.sh --from-dxspider /spider
```

See [Migration](migration.md) for details and current scope.

Current migration behavior also includes:

- a timestamped `migration-preflight` backup before a non-dry-run import
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

For support cases where the installation method or host layout is uncertain,
generate a redacted installation report:

```bash
sudo ./deploy/support-bundle.sh --redacted
```

See [Support Report](operations.md#support-report) for collected fields and the
explicit privacy mode, network/journal switches, consistent database export,
and sensitive offline instance archive.

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
- remove any authenticator-app TOTP secret
- leave the password unchanged
- write an audit record

If every sysop is locked out, recover locally on the host:

```bash
cd /home/pycluster/pyCluster
sqlite3 data/pycluster.db "
DELETE FROM mfa_challenges;
DELETE FROM user_prefs WHERE call='AI3I' AND pref_key IN ('mfa_email_otp','mfa_totp_secret');
INSERT INTO user_prefs(call,pref_key,pref_value,updated_epoch)
VALUES('AI3I','mfa_email_otp','off',strftime('%s','now'))
ON CONFLICT(call,pref_key) DO UPDATE SET
  pref_value=excluded.pref_value,
  updated_epoch=excluded.updated_epoch;
"
systemctl restart pycluster.service pyclusterweb.service
```

Replace `AI3I` with the principal/base sysop callsign you are recovering. If needed, also reset the password through the existing bootstrap or direct SQLite recovery path.

You can also use the host-local helper:

```bash
sudo /home/pycluster/pyCluster/scripts/lock_user_account.py --db /home/pycluster/pyCluster/data/pycluster.db --call AI3I --unlock
```

## Registration Recovery

Ordinary-user registration now tracks:

- registration state
- verified email status
- grace logins remaining before lockout

Self-registration from telnet or the public web UI validates the requested
principal before any registry or review-queue row is created. User-entered
registration callsigns must be simple ham-style callsigns with an optional
numeric SSID. Sysop-created local users and cluster peer accounts remain
managed through the System Console.

When SMTP is configured, registration verifies the user's email address before
the sysop review request is queued. If a verification code expires, the user can
run `REGISTER` again to request a new code.

Normal operator recovery paths:

- System Console user editor:
  - `Send Verification`
  - `Mark Verified`
  - `Unlock Account`
- Public web password reset:
  - available from the login popup
  - requires SMTP delivery to be configured
  - requires the account to already have a verified email address
  - sends a reset code, accepts a confirmed replacement password, and clears failed-password lock state
- Public web MFA reset:
  - is available separately from password reset in the login popup
  - requires the exact callsign/SSID, matching verified email, and an emailed recovery code
  - clears authenticator/MFA state without changing the password or unrelated lock state
  - retains verified email OTP when node policy requires MFA

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

You can manage retention from the System Operator web UI or run database cleanup manually through the UI action. Peer protocol traces under `logs/proto/` are pruned by the daily timer independently of database cleanup and default to 14 days; configure **Keep Protocol Logs For (days)** under Node Settings > Maintenance for smaller disks. Use **Protocol Log Detail** to select full frames, operational events only, or disabled logging.

## Log Rotation

The deploy scripts install logrotate coverage for:

- `/var/log/pycluster/authfail.log`

That policy rotates weekly, keeps compressed history, and prevents the auth-failure log from growing without bound on long-running nodes.

Peer protocol traces under `/home/pycluster/pyCluster/logs/proto/` use pyCluster's daily retention timer instead of logrotate. They are kept for 14 days by default; change **Keep Protocol Logs For (days)** under Node Settings > Maintenance when storage is constrained. Selecting **Events Only** prevents routine RX/TX frames from growing these files while preserving peer lifecycle and dropped-frame diagnostics.

## Default Ports

- telnet: `7300`
- sysop web: `8080`
- public web: `8081`

Default bind behavior:

- telnet listens publicly unless you change `telnet.host`
- the sysop web service listens on `127.0.0.1:8080`
- the public web service listens on `127.0.0.1:8081`

That localhost binding is intentional. A fresh install is not meant to expose the web UI directly until you finish reverse-proxy setup.
The reverse proxy may run locally or on another trusted host. External proxying requires explicit listener and firewall changes on the pyCluster VM.

## External Reverse Proxy / No Local nginx

pyCluster does not require nginx on its application host. A dedicated pyCluster VM can expose its backend listeners to a central nginx, HAProxy, Traefik, or similar reverse proxy elsewhere on the LAN.

Installed services load configuration from:

- `/home/pycluster/pyCluster/config/pycluster.toml`
- `/home/pycluster/pyCluster/config/pycluster.local.toml`

The checkout under `/usr/src/pyCluster` is the upgrade source. Editing its `config/` directory does not change the running services.

To run telnet on `7373`, publish the public/user web interface on `8081`, and make the System Operator interface available to a trusted proxy or management LAN on `8080`, create or edit `/home/pycluster/pyCluster/config/pycluster.local.toml`:

```toml
[telnet]
host = "0.0.0.0"
port = 7373
ports = [7373]

[web]
# System Operator interface. Restrict this port at the firewall.
host = "0.0.0.0"
port = 8080

[public_web]
# Public/user interface.
enabled = true
host = "0.0.0.0"
port = 8081
```

`[telnet].ports`, when non-empty, takes precedence over `[telnet].port`; set both as shown when replacing an existing multi-port configuration. Binding to the VM's specific LAN address is preferable to `0.0.0.0` when its address is stable.

For dual-stack IPv4 and IPv6 listeners on supported Linux systems, use an empty host string in each required section:

```toml
[telnet]
host = ""
port = 7373
ports = [7373]

[web]
host = ""
port = 8080

[public_web]
enabled = true
host = ""
port = 8081
```

`host = "::"` binds an IPv6-only wildcard socket, while `host = "::1"` remains IPv6 loopback-only. `host = ""` asks asyncio for separate IPv4 and IPv6 wildcard sockets and avoids relying on operating-system IPv4-mapped IPv6 behavior.

Restart and verify the effective listeners:

```bash
sudo systemctl restart pycluster.service pyclusterweb.service
sudo systemctl status pycluster.service pyclusterweb.service --no-pager
sudo ss -lntp | grep -E ':(7373|8080|8081)\b'
sudo /usr/src/pyCluster/deploy/doctor.sh
sudo journalctl -u pycluster.service -u pyclusterweb.service -n 100 --no-pager
```

Expected roles:

- `7373/tcp`: telnet cluster service
- `8080/tcp`: private System Operator web interface
- `8081/tcp`: public/user web interface

If `ss` shows `0.0.0.0:7373` but another LAN machine cannot connect, pyCluster is listening correctly. Check the VM firewall, NAS virtual switch, VLAN policy, and any upstream ACL; nginx does not participate in telnet traffic.

The IPv6 equivalent is a listener shown on `[::]:7373`. Confirm the client has a route to the VM, the VM has the expected global or ULA address, and IPv6 firewall policy permits the connection. Opening an IPv4 firewall rule does not necessarily open the equivalent IPv6 path.

Allow only the necessary sources. Example with UFW, where `192.0.2.10` is the proxy and `192.0.2.0/24` is the trusted LAN:

```bash
sudo ufw allow from 192.0.2.0/24 to any port 7373 proto tcp
sudo ufw allow from 192.0.2.10 to any port 8081 proto tcp
sudo ufw allow from 192.0.2.10 to any port 8080 proto tcp
```

When UFW has `IPV6=yes`, add source-scoped IPv6 rules for the trusted LAN or proxy address as well:

```bash
sudo ufw allow from 2001:db8:100::/64 to any port 7373 proto tcp
sudo ufw allow from 2001:db8:100::10 to any port 8081 proto tcp
sudo ufw allow from 2001:db8:100::10 to any port 8080 proto tcp
```

Equivalent firewalld rules can use source-scoped rich rules:

```bash
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="192.0.2.0/24" port port="7373" protocol="tcp" accept'
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="192.0.2.10/32" port port="8081" protocol="tcp" accept'
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="192.0.2.10/32" port port="8080" protocol="tcp" accept'
sudo firewall-cmd --reload
```

IPv6 firewalld rules use `family="ipv6"` and IPv6 source prefixes:

```bash
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv6" source address="2001:db8:100::/64" port port="7373" protocol="tcp" accept'
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv6" source address="2001:db8:100::10/128" port port="8081" protocol="tcp" accept'
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv6" source address="2001:db8:100::10/128" port port="8080" protocol="tcp" accept'
sudo firewall-cmd --reload
```

On the central nginx host, use separate hostnames for the public and System Operator interfaces. Replace `192.0.2.20` with the pyCluster VM address:

```nginx
map $http_upgrade $connection_upgrade_pycluster {
    default upgrade;
    '' close;
}

server {
    listen 443 ssl;
    server_name cluster.example.net;

    location / {
        proxy_pass http://192.0.2.20:8081;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade_pycluster;
    }
}

server {
    listen 443 ssl;
    server_name sysop-cluster.example.net;

    location / {
        proxy_pass http://192.0.2.20:8080;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade_pycluster;
    }
}
```

Configure certificates and access controls according to the central proxy's existing policy. Do not expose `8080` broadly or send System Operator credentials over an untrusted plain-HTTP network.

For an IPv6 backend, nginx requires brackets around the literal address:

```nginx
proxy_pass http://[2001:db8:100::20]:8081;
```

Publish matching `A` and/or `AAAA` records for the proxy's client-facing address as appropriate. The backend VM itself does not need public DNS when the central proxy reaches it over a private IPv4 address, ULA, or routed internal IPv6 prefix.

## Local nginx Reverse Proxy Setup

The supported reverse-proxy path is:

```bash
sudo ./deploy/setup-nginx.sh
```

You can also let `deploy/install.sh` call that for you interactively during first install.
That script is intended to claim `80/443` for the pyCluster nginx deployment path on the host. If another non-nginx service is already bound there, it stops with a clear error instead of silently fighting the existing web stack.

Typical nginx setup choices:

- `--public-host cluster.example.net`
- optional `--sysop-host sysop.example.net`
- `--tls-mode none`
- `--tls-mode self-signed`
- `--tls-mode letsencrypt --email admin@example.net`

The helper validates all supplied hostnames before writing nginx syntax. It writes pyCluster-owned nginx `server` blocks under `/etc/nginx/conf.d` by default. It does not edit the distribution default site in place; it disables the packaged default listener and installs pyCluster reverse-proxy files instead. Existing default and pyCluster configuration files are backed up for the duration of setup and restored automatically if `nginx -t`, restart, or certificate provisioning fails.

Expected files:

- `/etc/nginx/conf.d/pycluster-public.conf`
- `/etc/nginx/conf.d/pycluster-sysop.conf`

The public site proxies to `127.0.0.1:8081`. When `--sysop-host` is provided, the sysop site proxies to `127.0.0.1:8080`. Without `--sysop-host`, `pycluster-sysop.conf` is still written as an inert placeholder so the expected file exists, but it does not expose the sysop UI. Those backend listeners stay local; nginx is the public entry point.

Examples:

```bash
# Public web only, HTTP, useful for a private LAN or a lab node.
sudo ./deploy/setup-nginx.sh \
  --public-host pycluster.example.net \
  --tls-mode none

# Public web plus a separate sysop hostname, self-signed TLS.
sudo ./deploy/setup-nginx.sh \
  --public-host pycluster.example.net \
  --sysop-host sysop-pycluster.example.net \
  --tls-mode self-signed

# Public web plus Let's Encrypt. The email is required by Certbot.
sudo ./deploy/setup-nginx.sh \
  --public-host pycluster.example.net \
  --tls-mode letsencrypt \
  --email admin@example.net
```

Generated nginx shape:

```nginx
server {
    listen 80;
    server_name pycluster.example.net;

    location / {
        proxy_pass http://127.0.0.1:8081;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade_public;
    }
}
```

If `--sysop-host` is provided, the same pattern is generated for the sysop listener with `proxy_pass http://127.0.0.1:8080;`. Without `--sysop-host`, only the public web UI is exposed through nginx and the sysop config file remains a comment-only placeholder.

Common nginx and Let's Encrypt checks:

- confirm DNS for every requested hostname already points at the pyCluster host before using `--tls-mode letsencrypt`
- pass `--email you@example.net`; Certbot requires it and setup stops early when it is missing
- make sure ports `80` and `443` are reachable from the public Internet for HTTP-01 certificate validation
- run `sudo nginx -t` after manual edits
- run `sudo ./deploy/doctor.sh` to confirm pyCluster services, local health endpoints, and fail2ban state

If setup reports that ports `80` or `443` are already in use, identify the owner before retrying. pyCluster expects to own the host nginx path cleanly; do not overwrite a working site configuration unless you intentionally decided this host is dedicated to pyCluster.

## Optional Dependencies

Serial/KISS support:

```bash
pip install pyserial
```
