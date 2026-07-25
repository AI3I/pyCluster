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
- `deploy/fail2ban/filter.d/pycluster-auth-scanner.conf`

Included optional actions:

- `deploy/fail2ban/action.d/pycluster-lock-account.conf`

Included jails:

- `deploy/fail2ban/jail.d/pycluster-core.local`
- `deploy/fail2ban/jail.d/pycluster-web.local`
- `deploy/fail2ban/jail.d/pycluster-scanner.local`

Installed jail names:

- `pycluster-core-auth`
- `pycluster-web-auth`
- `pycluster-telnet-scanner`

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
sudo fail2ban-client status pycluster-web-auth
sudo fail2ban-client status pycluster-telnet-scanner
sudo tail -n 50 /var/log/pycluster/authfail.log
```

The default jails ban IP addresses. To also lock the user account named in a structured auth-failure line, enable the optional `pycluster-lock-account` action in a local jail override and point `pycluster_db` at the active SQLite database. The action writes the same durable lock keys that the SysOp `Unlock Account` control clears. The helper can also be run directly on the host:

```bash
sudo /opt/pycluster/current/scripts/lock_user_account.py --db /opt/pycluster/data/pycluster.db --call AI3I-90
sudo /opt/pycluster/current/scripts/lock_user_account.py --db /opt/pycluster/data/pycluster.db --call AI3I-90 --unlock
```

## Recommended Deployment Posture

- keep sysop and public web behind a reverse proxy
- use HTTPS externally
- enable the shipped `fail2ban` jails
- keep telnet exposed only as needed
- back up config and SQLite data regularly

## CTY and Security

CTY data is operational data, not a security boundary. Keep it current, but do not treat entity mapping as access control.
