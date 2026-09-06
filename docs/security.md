# Security

pyCluster uses layered security controls rather than a single mechanism.

## Current Layers

### Account and Access Controls

- per-user passwords
- first-login password creation for human telnet users
- per-user channel access
- per-user posting permissions
- callsign blocking
- block reason tracking
- durable failed-password lock state shared by telnet and public web login
- account-lock recovery notices from both telnet and public web when SMTP and a verified account email are configured
- durable five-attempt MFA lock state scoped to the exact callsign-SSID
- verified-email password reset through the public web when SMTP is configured
- authenticator MFA fallback to email OTP after repeated failed TOTP attempts

### Operator Visibility

- recent audit activity
- recent auth failures
- current fail2ban bans

### OS-Level Enforcement

pyCluster ships auth-failure patterns for `fail2ban`.

Included filters:

- `deploy/fail2ban/filter.d/pycluster-auth-core.conf`
- `deploy/fail2ban/filter.d/pycluster-auth-web.conf`
- `deploy/fail2ban/filter.d/pycluster-auth-telnet.conf`
- `deploy/fail2ban/filter.d/pycluster-auth-scanner.conf`

Included optional actions:

- `deploy/fail2ban/action.d/pycluster-lock-account.conf`

Included jails:

- `deploy/fail2ban/jail.d/pycluster-core.local`
- `deploy/fail2ban/jail.d/pycluster-web.local`
- `deploy/fail2ban/jail.d/pycluster-telnet.local`
- `deploy/fail2ban/jail.d/pycluster-scanner.local`

Installed jail names:

- `pycluster-core-auth`
- `pycluster-telnet-auth`
- `pycluster-web-auth`
- `pycluster-telnet-scanner`

The SysOp-web and telnet authentication jails each use five failed attempts, but they ban only their own service ports. Password failures for an exact account with verified email and configured recovery mail are handled by pyCluster's durable account lock and are excluded from the public-web and telnet IP bans, allowing immediate reset and reconnection. Malformed, unverified, and mail-unrecoverable attempts remain eligible for IP bans. Password and MFA recovery each require the exact callsign/SSID, matching verified email, and a purpose-bound one-time code, so records sharing an email address cannot reset one another. MFA recovery does not change the password, clear unrelated locks, or bypass node-wide MFA policy. Failed-password and failed-MFA locks are recorded against the exact callsign-SSID; a failure for one SSID does not lock its base call or siblings. The scanner jail matches malformed callsigns only, and installation leaves any existing SSH jail enabled.

## Auth Failure Logging

pyCluster emits structured auth-failure lines suitable for `fail2ban`.

Example:

```text
AUTHFAIL channel=sysop-web ip=198.51.100.24 call=N0CALL reason=bad_password
```

Common reasons:

- `bad_password`
- `invalid_credentials`
- `invalid_callsign`
- `blocked_login`
- `web_login_not_allowed`
- `telnet_login_not_allowed`

Operational checks:

```bash
sudo fail2ban-client status
sudo fail2ban-client status pycluster-core-auth
sudo fail2ban-client status pycluster-telnet-auth
sudo fail2ban-client status pycluster-web-auth
sudo fail2ban-client status pycluster-telnet-scanner
sudo tail -n 50 /var/log/pycluster/authfail.log
```

The default jails ban IP addresses. To also lock the user account named in a structured auth-failure line, enable the optional `pycluster-lock-account` action in a local jail override and point `pycluster_db` at the active SQLite database. The action writes the same durable lock keys that the SysOp `Unlock Account` control clears. The helper can also be run directly on the host:

```bash
sudo /home/pycluster/pyCluster/scripts/lock_user_account.py --db /home/pycluster/pyCluster/data/pycluster.db --call AI3I-90
sudo /home/pycluster/pyCluster/scripts/lock_user_account.py --db /home/pycluster/pyCluster/data/pycluster.db --call AI3I-90 --unlock
```

## Recommended Deployment Posture

- keep sysop and public web behind a reverse proxy
- use HTTPS externally
- a reverse proxy may run on the pyCluster host or a separate trusted host; for an external proxy, bind backend listeners explicitly and allow their ports only from the proxy or management network
- dual-stack listeners require both IPv4 and IPv6 firewall review; do not assume an IPv4-only ACL also protects a wildcard IPv6 socket
- never expose the System Operator listener on `8080` broadly or send its credentials over an untrusted plain-HTTP network
- enable the shipped `fail2ban` jails
- keep telnet exposed only as needed
- back up config and SQLite data regularly

## CTY and Security

CTY data is operational data, not a security boundary. Keep it current, but do not treat entity mapping as access control.
## Local Address Blocks

System Operators can manage address blocks under **Telemetry > Security >
Address Blocks**, or with `sysop/ipblock show`,
`sysop/ipblock add <IP/CIDR> <minutes> <reason>`, and
`sysop/ipblock remove <id>`. Zero minutes means permanent. IPv4, IPv6, and
IPv4-mapped IPv6 are supported. CIDRs are normalized to network boundaries.

Blocks apply before a new telnet login and before web request bodies or
authentication are processed, including System Operator web requests. They do
not terminate existing telnet or WebSocket sessions or outbound peer links.
Changes, expiration, and removal history are stored in SQLite. These are
application-level rules; they do not install firewall rules or replace Fail2Ban.
Blocking your own address or network can prevent further web access; use an
unblocked operator connection to remove the rule.

Both `[web]` and `[public_web]` accept `trusted_proxies`, an array of IPs/CIDRs.
The default is `["127.0.0.1/32", "::1/128"]` for the bundled local nginx setup.
For an external reverse proxy, configure its exact address in each applicable
section of `pycluster.local.toml` and restart the corresponding service. An
empty array disables forwarded-address trust. The client address is resolved
from right to left through `X-Forwarded-For`, stopping at the first untrusted
hop. Headers from direct untrusted clients cannot override their socket IP.
