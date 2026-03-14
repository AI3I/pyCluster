# Telnet Commands

pyCluster keeps a DX-style telnet interface, but aims to make the output more readable than many legacy clusters.

## Login Model

- normal users log in with callsign and password
- first telnet login for a new human user requires password creation
- node-classified records skip password prompts for node-to-node use
- sysop sessions get the `#` prompt

## Common User Commands

### Spot Viewing

Examples:

```text
sh/dx 10
sh/dx K3AJ exact
sh/dx by WW5L
sh/dx on 40m
sh/dx info RTTY
```

### Station/Profile

Examples:

```text
set/name John Lewis
set/qth Western Pennsylvania
set/qra FN00FS
set/email dxcluster@example.net
set/homenode AI3I-16
set/password mynewpass
unset/password
```

### Session Preferences

Examples:

```text
set/echo
unset/echo
set/beep
unset/beep
set/language de
set/page 20
set/nowrap
unset/nowrap
```

### Filters and Personal Lists

Examples:

```text
accept/spots 20m
reject/spots FT8
clear/spots
set/buddy K3AJ
show/buddy
unset/buddy K3AJ
```

### Messages and Traffic

Examples:

```text
send K3AJ Hello from pyCluster
read 12
reply 12 Copy, thanks
show/messages
```

## Operator Commands

Privileged actions live under `sysop/*`.

Examples:

```text
sysop/users
sysop/sysops
sysop/showuser AI3I
sysop/password AI3I newpass
sysop/clearpassword AI3I
sysop/user AI3I qth Western Pennsylvania
sysop/user AI3I node_family dxspider
sysop/privilege AI3I sysop
sysop/access AI3I
sysop/setaccess AI3I web login on
sysop/connect N9JR-2 dxspider://dx.n9jr.com:7300?login=AI3I-16&client=N9JR-2
sysop/disconnect N9JR-2
sysop/audit
```

## Discovery Commands

Examples:

```text
help
apropos route
show/commands
show/shortcuts
```

## Operator Visibility Commands

Examples:

```text
show/links
show/node
show/users
show/connect
show/log
show/proto
show/protoalerts
show/protohistory
```

## Notes

- command compatibility exists for many DXSpider-style names
- not every recognized command is fully implemented
- pyCluster tries to return explicit, human-readable responses when a command is accepted but not implemented
