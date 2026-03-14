# Operations

This page covers the day-to-day operator view of a deployed pyCluster node.

## Services

Typical production services:

- `pycluster.service`
- `pyclusterweb.service`
- `pycluster-cty-refresh.timer`

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
- public branding response

## Backups

At minimum, back up:

- `config/pycluster.toml`
- `data/pycluster.db`
- local CTY overrides if you have any

## CTY Refresh

Manual:

```bash
python3 ./scripts/update_cty.py --config ./config/pycluster.toml
```

Automatic:

- `pycluster-cty-refresh.timer`

## Security Operations

### fail2ban

pyCluster ships `fail2ban` filters and jails for auth-failure events.

Relevant repo paths:

- `deploy/fail2ban/filter.d/pycluster-auth-core.conf`
- `deploy/fail2ban/filter.d/pycluster-auth-web.conf`
- `deploy/fail2ban/jail.d/pycluster-core.local`
- `deploy/fail2ban/jail.d/pycluster-web.local`

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
