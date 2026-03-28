# Telnet Commands

pyCluster keeps a DX-style telnet interface, but tries to make it more teachable and more readable than many legacy cluster systems.

## Login Model

- normal users log in with callsign and password
- first telnet login for a new human user requires password creation
- node-classified records skip password prompts for node-to-node use
- sysop sessions get the `#` prompt

## Prompt Templates

The prompt is configurable through `node.prompt_template` and `sysop/setprompt`.

Available tokens:

- `{timestamp}`
- `{node}`
- `{callsign}`
- `{suffix}`

Example:

```text
[{timestamp}] {node}{suffix}
```

## Discovery First

Start here on a live node:

```text
help
show/commands
show/shortcuts
apropos route
```

`show/commands` lists real commands grouped by family. Use `show/commands set` or `show/commands route` to narrow the list.

Those commands are privilege-aware, so ordinary users are not flooded with sysop-only actions.

## Everyday Commands

Most users will spend their time in a small subset of the command surface:

```text
sh/dx 10
show/messages
send K9JR Hello from pyCluster
set/name Example Operator
set/qth Example City
set/qra FN31PR
set/homenode N0CALL-1
set/password mynewpass
set/page 20
set/nowrap
show/users
show/node
show/links
show/sun
show/grayline
show/moon
```

Solar views use stored QRA/grid information when available. If no grid is known, the node grid square is used as a fallback.

## Operator Commands

Privileged actions live under `sysop/*`:

```text
sysop/users
sysop/sysops
sysop/showuser N0CALL
sysop/password N0CALL newpass
sysop/clearpassword N0CALL
sysop/user N0CALL qth Example City
sysop/privilege N0CALL sysop
sysop/access N0CALL
sysop/setaccess N0CALL web login on
sysop/audit
sysop/services
sysop/restart telnet
sysop/setprompt [{timestamp}] {node}{suffix}
```

By default, `Non-Authenticated` users may log in but cannot post DX spots or announces until access is raised or overridden with `sysop/setaccess`.

## Full Command Reference

For the full documented command surface, grouped by family, see:

- [Telnet Command Reference](telnet-command-reference.md)

## Notes

- pyCluster recognizes many DXSpider-style command names for compatibility
- the documented reference focuses on implemented, operator-meaningful behavior
- obsolete compatibility names such as `mrtg`, `gtk`, and `ve7cc` are no longer part of the active command surface
