# System Operator Web

The System Operator web console is the browser-based control surface for node operations.

Default local URL:

- `http://127.0.0.1:8080/sysop/`

## What It Covers

- node presentation and MOTD
- users and access matrix
- peer and link management
- protocol summary, history, and policy drops
- runtime stats
- audit trail
- security view for failed logins and fail2ban bans

## Login

Use a local callsign that has:

- a configured password
- `System Operator` access

## Main Areas

### Node

Manage:

- node call and alias
- owner name
- QTH and locator
- MOTD
- support contact
- branding and public presentation

### Users

Manage:

- local users
- blocked users
- system operators
- per-user telnet/web access
- posting permissions
- password set/clear
- home node
- notes or block reason

### Peers and Links

Manage:

- dial-out peer definitions
- transport address
- cluster family
- retry behavior
- optional peer password
- live connect/disconnect state

### Protocol Health

Shows:

- protocol summary
- policy drops
- protocol history
- thresholds and acknowledgements

### Telemetry

Shows:

- at-a-glance stats
- runtime stats
- recent audit
- security events

## Security View

The security section shows:

- recent auth failures
- current fail2ban bans

This complements, but does not replace:

- local callsign blocking
- fail2ban enforcement
- reverse-proxy and firewall controls
