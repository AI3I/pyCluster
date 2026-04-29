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

## Recommended Deployment Posture

- keep sysop and public web behind a reverse proxy
- use HTTPS externally
- enable the shipped `fail2ban` jails
- keep telnet exposed only as needed
- back up config and SQLite data regularly

## CTY and Security

CTY data is operational data, not a security boundary. Keep it current, but do not treat entity mapping as access control.
