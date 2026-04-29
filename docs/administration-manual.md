# Administration Manual

This manual is for System Operators running a pyCluster node.

It is intended to be closer in spirit to traditional DX cluster administration documentation: task-oriented, operator-focused, and separated from normal user docs.

## 1. What a Sysop Manages in pyCluster

A System Operator is responsible for:

- node identity and presentation
- local users and system operators
- access control
- blocked users
- peer links
- protocol health
- posting and operator tools
- security visibility
- deployment and service health

In practice that means a sysop should be able to:

- control what users see when they log in
- manage passwords and privileges
- define and troubleshoot peers
- understand protocol health without raw protocol spelunking
- respond to abuse or repeated bad logins

## 2. System Operator Access

System Operator access exists in two places:

- telnet, through `sysop/*`
- the System Operator web console

Sysop telnet sessions use:

```text
N0CALL-1#
```

System Operator web login requires:

- a local password
- `System Operator` privilege on the node

Typical privileged telnet session:

```text
login: N0CALL
password:
Welcome to pyCluster on N0CALL-1
N0CALL-1#
```

The `#` prompt is the main visual indicator that the session is privileged.

## 3. Node Settings

Use the `Node Settings` section of the sysop web console to manage:

- node call and alias
- owner name
- QTH and grid
- branding and welcome text
- telnet ports
- MOTD
- support contact
- website URL
- telnet login behavior such as post-MOTD status display

Typical uses of this section:

- updating the MOTD for maintenance or contest weekends
- changing telnet ports during a migration
- updating support contact and public branding
- changing welcome text before opening the node to new users

See:

- [System Operator Web](sysop-web.md)

## 4. User Administration

pyCluster keeps local user administration inside the node itself.

Main tasks:

- create users
- update profile data
- set or clear passwords
- assign `System Operator` access
- block and unblock users
- set access policy by channel/capability

Key ideas:

- `Blocked` is separate from normal users
- system operators are shown separately from ordinary local users
- access matrix controls where users can log in and what they may post

Common workflow: create or update a user

1. open `Users`
2. click `New User` or select an existing record
3. fill in profile details
4. choose the access level
5. click `Save User`

Common workflow: block a user

1. open the user record
2. change `Access Level` to `Blocked`
3. add a short note or block reason
4. save the record

That block applies to the base callsign and matching SSID variants.

Useful telnet commands:

```text
sysop/users
sysop/sysops
sysop/showuser N0CALL
sysop/password N0CALL newpass
sysop/clearpassword N0CALL
sysop/privilege N0CALL sysop
sysop/blocklogin N0CALL on
sysop/access N0CALL
sysop/setaccess N0CALL web login on
```

Use telnet for these actions when:

- you are working from a terminal-only environment
- you want a quick one-off operator action
- you want to verify privilege-gated behavior directly

## 5. Peer and Link Administration

pyCluster separates:

- transport address
- cluster family

This avoids conflating:

- how to connect
- how to behave after connection

Peer roles:

- `Dial-out`
- `Accepted`

Operational tasks:

- define saved peers
- connect or disconnect peers
- view live traffic and health
- inspect policy drops
- adjust retry behavior
- delete saved peer definitions that are no longer needed

Typical outbound-peer workflow:

1. click `New Peer`
2. enter:
   - peer name
   - transport address
   - cluster family
3. optionally set a peer password
4. save the peer
5. connect it

Important distinction:

- `Transport Address`
  - how pyCluster opens the connection
- `Cluster Family`
  - how pyCluster behaves once the connection is established

Health distinction:

- connected/disconnected answers whether the transport socket is live
- inbound/accepted versus dial-out answers who initiated the socket
- traffic labels such as bidirectional, receive active, transmit active, idle, and connected quiet describe recent traffic direction
- protocol-health labels such as stale, degraded, or flapping are based on received PC protocol freshness and thresholds

See:

- [Node Linking](node-linking.md)
- [System Operator Web](sysop-web.md)

## 6. Protocol Health and Troubleshooting

The `Protocol Health` area is where operators inspect:

- tracked peers
- health state
- alerts
- acknowledgements
- policy drops
- protocol history

It also controls thresholds such as:

- stale minutes
- flap score
- flap window

Useful telnet commands:

```text
show/proto
show/protohistory
show/protoalerts
show/protoacks
show/policydrop
```

Typical troubleshooting flow:

1. inspect `show/links` or the web peer table
2. check protocol summary health
3. inspect policy drops
4. review protocol history
5. only then decide whether thresholds need adjustment

Spot review note:

- pyCluster now ingests plausible spot calls even when they are not recognized by the currently loaded prefix dataset
- suspicious calls are flagged in the System Console spot table with a `Review` badge
- the app log records these as `spot call review: ...` entries so sysops can audit which peer and callsign triggered the review signal

## 7. Security and Abuse Control

pyCluster uses layered security:

- callsign/password auth
- per-user access control
- blocked users
- structured auth-failure logging
- `fail2ban` integration

The sysop web `Security` section shows:

- recent auth failures
- current bans

Typical login-abuse workflow:

1. review recent auth failures
2. see whether `fail2ban` has already banned the source
3. block the callsign locally if needed
4. adjust user access if the issue is capability abuse rather than login abuse

This is meant to keep the operator informed without requiring direct log-tail-only workflows.

See:

- [Security](security.md)

## 8. Runtime and Audit

The `Telemetry` section groups:

- runtime stats
- recent spots
- recent audit
- security events

Useful telnet commands:

```text
sysop/audit
sysop/services
sysop/restart telnet
show/log
show/users
show/links
```

Use audit and telemetry when:

- you need to know who changed a setting
- you need to confirm an operator action happened
- you need to understand recent runtime or security events quickly

## 9. Deployment and Operations

Supported operational scripts:

- `deploy/install.sh`
- `deploy/upgrade.sh`
- `deploy/repair.sh`
- `deploy/uninstall.sh`
- `deploy/doctor.sh`

Services:

- `pycluster.service`
- `pyclusterweb.service`
- `pycluster-data-refresh.timer`

Healthy baseline:

- core service active
- public web service active
- nginx active when the host is publishing web UI on `80/443`
- CTY refresh timer active
- wpxloc.raw configured and current if you use DXSpider-style WPX/location data
- database present
- security logging and `fail2ban` functioning

See:

- [Installation](installation.md)
- [Operations](operations.md)

## 10. Reference Material

For detailed command coverage, use:

- [Telnet Command Reference](telnet-command-reference.md)
- [System Operator Web](sysop-web.md)
- [Node Linking](node-linking.md)
- [Security](security.md)


Dataset status is visible in the System Operator Console Node Settings view and in telnet `show/configuration`. Use that before treating unknown prefixes as suspicious; stale CTY or missing `wpxloc.raw` data can make review cues less reliable.
