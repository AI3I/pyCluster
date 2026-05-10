# Command Specification

This document defines the intended pyCluster command surface. It is narrower than the historical DXSpider-compatible command catalog.

The telnet command reference documents what the current implementation recognizes. This specification defines what should be treated as first-class product behavior, what exists only for compatibility, and what should be hidden or removed as the command surface is cleaned up.

## Goals

pyCluster should keep commands operators and users actually need:

- post and read DX cluster traffic
- maintain local user profile data
- inspect useful local and network state
- configure pyCluster features that have real backend behavior
- operate and troubleshoot node links
- administer users, access, policy, and service health

pyCluster should not mirror legacy command families just because they existed elsewhere. A command that only stores a value, prints a placeholder, or has no meaningful local behavior is not a supported product feature.

## Command Classes

### Stable

Stable commands are part of the supported pyCluster interface. They must be documented, tested, and backed by real behavior.

Requirements:

- the command reads or writes a real pyCluster database, configuration, runtime, or network state
- help text and examples are accurate
- permission behavior is explicit
- `set/*`, `unset/*`, and `show/*` forms are consistent when all three make sense
- failures explain what is missing or why the command cannot act

### Compatibility

Compatibility commands exist to make legacy habits less painful, but they are not the preferred pyCluster interface.

Requirements:

- the command maps to a stable pyCluster behavior or emits a clear compatibility response
- command discovery should not promote it as a primary workflow
- documentation should identify the preferred pyCluster replacement
- tests should cover only the compatibility mapping and failure mode

### Deprecated

Deprecated commands are still accepted temporarily, but should be removed after a published cleanup window.

Requirements:

- output should say the command is deprecated and name the replacement when one exists
- command discovery should hide it by default
- release notes should list the deprecation
- tests should verify the warning and replacement text

### Removed

Removed commands should return `?` or a clear unsupported-command response. They should not silently store inert preferences or imply unsupported behavior.

## Stable User Commands

These commands are user-facing and should remain visible in normal help and command discovery.

### Discovery and Session

- `help`
- `show/commands [term]`
- `show/apropos <term>`
- `show/shortcuts [term]`
- `show/version`
- `show/program`
- `show/date`
- `show/time`
- `show/uptime`
- `show/motd`
- `ping`
- `bye`

### DX and Cluster Visibility

- `dx <freq_khz> <call> [comment]`
- `show/dx [filters]`
- `show/node [call]`
- `show/cluster`
- `show/users`
- `show/links`
- `show/connect`
- `show/route`
- `show/hops`
- `show/bands`
- `show/dxstats`
- `show/hfstats`
- `show/vhfstats`
- `show/hftable`
- `show/vhftable`
- `show/prefix <prefix>`
- `show/lastspot <call>`
- `show/dxcc <prefix>`
- `show/qrz <call>`
- `show/wm7d <call>`

### Propagation, Solar, Weather, and Satellite Views

- `show/sun`
- `show/moon`
- `show/grayline`
- `show/muf`
- `show/wcy`
- `show/wwv`
- `show/wx`
- `show/satellite [target]`

### Traffic and Messaging

- `announce <text>`
- `talk <call> <text>`
- `send <call> <text>`
- `msg <call> <text>`
- `read <id>`
- `reply <id> <text>`
- `show/messages`
- `show/mail`
- `show/outbox`
- `show/msgstatus`
- `show/announce`
- `show/chat`

### Local Profile and Preferences

- `set/name <text>`
- `set/qth <text>`
- `set/qra <grid>`
- `set/location <text>`
- `set/address <text>`
- `set/email <address>`
- `unset/email`
- `set/homenode <node>`
- `set/homebbs <bbs>`
- `set/password <newpass>`
- `unset/password`
- `set/page <lines>`
- `set/language <code>`
- `set/echo` / `unset/echo`
- `set/here` / `unset/here`
- `set/beep` / `unset/beep`
- `set/nowrap` / `unset/nowrap`
- `set/logininfo` / `unset/logininfo`
- `show/station [call]`
- `show/registered [call]`
- `show/configuration`

### User Lists, Variables, and Filters

These are stable only if they are backed by stored local data and visible behavior.

- `set/buddy <call>`
- `unset/buddy <call>`
- `show/buddy`
- `set/var <name> <value>`
- `unset/var <name>`
- `show/var [name]`
- `set/uservar <name> <value>`
- `unset/uservar <name>`
- `set/usdb <field> <value>`
- `delete/usdb <field>`
- `show/usdb [call]`
- `accept/spots <expr>`
- `reject/spots <expr>`
- `clear/spots`
- `show/filter`

## Stable Sysop Commands

Sysop commands should be intentionally pyCluster-shaped. Prefer `sysop/*` for administrative state over legacy user-visible toggles.

### User and Access Administration

- `sysop/users`
- `sysop/sysops`
- `sysop/showuser <call>`
- `sysop/user <call> <field> <value>`
- `sysop/deleteuser <call>`
- `sysop/password <call> <newpass>`
- `sysop/clearpassword <call>`
- `sysop/clearmfa <call>`
- `sysop/privilege <call> <level>`
- `sysop/homenode <call> <node>`
- `sysop/blocklogin <call> <on|off>`
- `sysop/access <call>`
- `sysop/setaccess <call> <channel|all> <capability|all> <on|off>`
- `sysop/spotlimit <default|call> ...`

### Runtime, Audit, and Health

- `sysop/audit [category] [limit]`
- `sysop/services`
- `sysop/restart <telnet|sysopweb|all>`
- `sysop/path <call|peer>`
- `sysop/peeraccount add <peer-call> <pycluster|dxspider|dxnet|arcluster|clx>`
- `sysop/peeraccount password <peer-call> <password>`
- `sysop/peeraccount show <peer-call>`
- `sysop/peer show [peer]`
- `sysop/peer add <peer> <dsn> [profile]`
- `sysop/peer set <peer> <dsn|profile|password|retry> <value>`
- `sysop/peer delete <peer>`
- `sysop/peer connect <peer>`
- `sysop/peer disconnect <peer>`
- `sysop/peerprofile <peer> <pycluster|dxspider|dxnet|arcluster|clx>`
- `show/log [category] [limit]`
- `show/files`
- `show/policy`
- `show/policydrop`

### Node Link and Protocol Management

Preferred operator workflow:

- create, edit, delete, connect, and disconnect saved peers in the System Operator web console
- use telnet for quick inspection and emergency operations

Stable telnet commands:

- `connect <peer> <dsn>`
- `disconnect <peer>`
- `links`
- `show/links`
- `show/capabilities`
- `show/proto`
- `show/protohistory`
- `show/protoevents`
- `show/protoalerts`
- `show/protoacks`
- `show/protoconfig`
- `set/protothreshold <key> <value>`
- `unset/protothreshold <key>`
- `set/protoack <peer|all>`
- `unset/protoack <peer|all>`
- `clear/protohistory`

## Sysop Command Gaps

These are the places where current behavior does not yet match the intended pyCluster model.

### Peer Management

Current state:

- the web console can save peers, delete saved peers, connect, disconnect, and set peer family
- inbound node-link acceptance depends on a local account for the remote node callsign with `node_family` set
- outbound node-link success depends on the remote node having an account for this node's login callsign
- telnet has explicit sysop peer-account and saved-peer commands
- telnet also has direct `connect` and `disconnect` compatibility commands
- telnet peer family is controlled through legacy-looking commands such as `set/dxspider [peer]`

Implemented sysop commands:

- `sysop/peeraccount add <peer-call> <pycluster|dxspider|dxnet|arcluster|clx>`
- `sysop/peeraccount password <peer-call> <password>`
- `sysop/peeraccount show <peer-call>`
- `sysop/peer add <peer> <dsn> [profile]`
- `sysop/peer set <peer> <dsn|profile|password|retry> <value>`
- `sysop/peer delete <peer>`
- `sysop/peer connect <peer>`
- `sysop/peer disconnect <peer>`
- `sysop/peer show [peer]`
- `sysop/peerprofile <peer> <pycluster|dxspider|dxnet|arcluster|clx>`

Disposition:

- keep the web console as the primary workflow
- document that saved peer definitions and node-login accounts are separate requirements
- make `set/dxspider [peer]`, `set/dxnet [peer]`, `set/arcluster [peer]`, and `set/clx [peer]` compatibility aliases only

### Node Configuration

Current state:

- many node settings are controlled in the System Operator web console
- telnet has scattered `set/*` compatibility commands that can look like node configuration but often only store local preferences

Needed sysop commands:

- `sysop/config show [section]`
- `sysop/config set <section.key> <value>`
- `sysop/config unset <section.key>`
- `sysop/smtp test <email>`
- `sysop/qrz test <call>`
- `sysop/data refresh [cty|wpxloc|all]`
- `sysop/data status`

Disposition:

- do not promote legacy `set/*` names for node-wide settings
- make node-wide config explicit, privileged, and auditable

### Security and Abuse Controls

Current state:

- access matrix, block-login, MFA reset, and spot limits exist
- fail2ban visibility is available in the web console

Needed sysop commands:

- `sysop/security show`
- `sysop/security failures [limit]`
- `sysop/security bans`
- `sysop/security unban <ip>`
- `sysop/user lock <call> [reason]`
- `sysop/user unlock <call>`

Disposition:

- keep `sysop/blocklogin` as compatibility or a thin alias
- prefer explicit `sysop/user lock|unlock` once implemented

## Compatibility Commands to Keep

These commands are acceptable aliases because they map directly to stable behavior.

- `sh/dx` -> `show/dx`
- `links` -> `show/links`
- `mail` -> `show/messages`
- `version` -> `show/version`
- `users` -> `show/users`
- `node` -> `show/node`
- `cluster` -> `show/cluster`
- `ap` and `apropos` -> `show/apropos`
- `stat/*` commands that report real database/runtime summaries
- `show/protoack`, `show/prack`, `set/prack`, and `unset/prack` as aliases for protocol acknowledgements

## Compatibility Commands to Hide or Deprecate

The current registry recognizes hundreds of commands. A local scan of `telnet_server.py` shows 372 grouped commands, including 83 `set/*`, 61 `unset/*`, and 141 `show/*` entries. At least 46 `set/*` commands and 35 `unset/*` commands are generic named-variable wrappers rather than clearly designed pyCluster features.

These should not be promoted in normal help. Each should either graduate to stable behavior with tests and documentation, or move to deprecated/removed.

### Legacy Protocol and Routing Toggles

Candidates:

- `set/wantpc16`, `unset/wantpc16`, `show/wantpc16`
- `set/wantpc9x`, `unset/wantpc9x`, `show/wantpc9x`
- `set/sendpc16`, `unset/sendpc16`, `show/sendpc16`
- `set/routepc19`, `unset/routepc19`, `show/routepc19`
- `set/senddbg`, `unset/senddbg`, `show/senddbg`
- `set/agwengine`, `unset/agwengine`, `show/agwengine`
- `set/agwmonitor`, `unset/agwmonitor`, `show/agwmonitor`
- `set/believe`, `unset/believe`, `show/believe`

Disposition:

- hide from ordinary command discovery immediately
- keep only if linked to real protocol/link behavior
- otherwise deprecate for removal

### Legacy Duplicate and Internal Flags

Candidates:

- `set/dupann`, `unset/dupann`, `show/dupann`
- `set/dupeph`, `unset/dupeph`, `show/dupeph`
- `set/dupspots`, `unset/dupspots`, `show/dupspots`
- `set/dupwcy`, `unset/dupwcy`, `show/dupwcy`
- `set/dupwwv`, `unset/dupwwv`, `show/dupwwv`
- `clear/dupefile`

Disposition:

- keep duplicate suppression as core engine behavior
- expose operational visibility through `show/policy`, `show/policydrop`, and sysop web
- do not expose legacy low-level duplicate toggles unless a real pyCluster use case is defined

### Legacy User or Node Flags With Weak Semantics

Candidates:

- `set/announce`, `unset/announce`
- `set/anntalk`, `unset/anntalk`
- `set/dx`, `unset/dx`
- `set/dxcq`, `unset/dxcq`
- `set/dxitu`, `unset/dxitu`
- `set/dxgrid`, `unset/dxgrid`
- `set/rbn`, `unset/rbn`
- `set/talk`, `unset/talk`
- `set/wcy`, `unset/wcy`
- `set/wwv`, `unset/wwv`
- `set/wx`, `unset/wx`
- `set/isolate`, `unset/isolate`
- `set/lockout`, `unset/lockout`
- `set/register`, `unset/register`
- `set/localnode`, `unset/localnode`
- `set/bbs`, `unset/bbs`
- `set/hops`, `unset/hops`
- `set/obscount`
- `set/usstate`, `unset/usstate`
- `set/pinginterval`
- `set/passphrase`, `unset/passphrase`

Disposition:

- keep only the commands that affect actual user-visible behavior
- move access control to `sysop/access` and `sysop/setaccess`
- move node policy to sysop web and explicit `sysop/*` commands
- remove or hide commands that only store inert preferences

### Peer Family Commands

Current behavior:

- `set/dxspider [peer]`
- `set/dxnet [peer]`
- `set/arcluster [peer]`
- `set/clx [peer]`
- `unset/dxnet`
- `unset/arcluster`
- `unset/clx`

Problem:

- the command name looks like ordinary user preference when it can also target peer behavior
- `set/dxspider UR2EZ` reports `Peer UR2EZ was not found` if the named peer does not exist
- there is no matching `unset/dxspider`, because DXSpider is the fallback profile
- this is confusing and should not be a primary configuration path

Disposition:

- keep peer family configuration in the System Operator web console as the primary workflow
- add explicit sysop commands if telnet peer-family editing remains needed
- treat `set/dxspider [peer]`, `set/dxnet [peer]`, `set/arcluster [peer]`, and `set/clx [peer]` as compatibility aliases
- update failure text to say `No saved or live peer named <peer>; use the SysOp web Peers and Links view or create/connect the peer first`
- hide these from ordinary user help

## Documentation Rules

Documentation should follow this order:

1. `docs/command-specification.md` defines intended support and cleanup policy.
2. `docs/telnet-command-reference.md` lists stable and compatibility commands that users may actually type.
3. `docs/dxspider-command-catalog.md` and `docs/dxspider-parity-matrix.md` remain historical compatibility research, not product promises.
4. README examples should use only stable commands.

README and quick-start docs should avoid legacy aliases unless the alias is intentionally part of the stable user experience.

## Cleanup Plan

Recommended release sequencing:

1. In `1.0.8`, hide deprecated compatibility commands from ordinary `show/commands`, leaving `show/notimpl` and a sysop/debug view for inventory.
2. Add a command metadata table in code with fields for status, audience, capability, replacement, and visibility.
3. Update command execution so deprecated commands emit a warning and replacement.
4. Remove inert named-variable wrappers that do not back real behavior.
5. Replace peer-family `set/<family> [peer]` usage with explicit sysop peer-profile commands or web-only management.
6. Keep tests for stable behavior and compatibility aliases; delete tests that only preserve legacy clutter.

## Acceptance Criteria

A command is fit to remain stable when:

- it has a documented purpose in this specification
- it appears in the telnet command reference with accurate syntax
- it is discoverable at the right privilege level
- it has focused tests for success and failure paths
- it changes, reads, or reports real pyCluster state
- it has a clear web-console equivalent when the task is operational/sysop oriented
