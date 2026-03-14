# Telnet Commands

pyCluster keeps a DX-style telnet interface, but tries to make it more teachable and more readable than many legacy cluster systems.

## Login Model

- normal users log in with callsign and password
- first telnet login for a new human user requires password creation
- node-classified records skip password prompts for node-to-node use
- sysop sessions get the `#` prompt

## Discovery First

Start here on a live node:

```text
help
show/commands
show/shortcuts
apropos route
```

Those commands are privilege-aware, so ordinary users are not flooded with sysop-only actions.

## Everyday Commands

Most users will spend their time in a small subset of the command surface:

```text
sh/dx 10
show/messages
send K3AJ Hello from pyCluster
set/name John Lewis
set/qth Western Pennsylvania
set/qra FN00FS
set/homenode AI3I-16
set/password mynewpass
set/page 20
set/nowrap
show/users
show/node
show/links
```

## Operator Commands

Privileged actions live under `sysop/*`:

```text
sysop/users
sysop/sysops
sysop/showuser AI3I
sysop/password AI3I newpass
sysop/clearpassword AI3I
sysop/user AI3I qth Western Pennsylvania
sysop/privilege AI3I sysop
sysop/access AI3I
sysop/setaccess AI3I web login on
sysop/audit
sysop/services
sysop/restart telnet
```

## Full Command Reference

For the full documented command surface, grouped by family, see:

- [Telnet Command Reference](telnet-command-reference.md)

## Notes

- pyCluster recognizes many DXSpider-style command names for compatibility
- the documented reference focuses on implemented, operator-meaningful behavior
- some compatibility names may still return a clear not-implemented response instead of legacy behavior
