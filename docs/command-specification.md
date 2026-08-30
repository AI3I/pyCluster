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

## Database Ownership Model

The database should be the source of truth for any state that can be changed from more than one surface. Telnet commands, the public web UI, the System Operator web UI, and node-link ingestion should all read and write the same store records instead of keeping separate preference models.

State ownership by command family:

| State | Database owner | Telnet surface | Web surface | Notes |
| --- | --- | --- | --- | --- |
| User profile | `user_registry`, `user_prefs` | `set/name`, `set/qth`, `set/qra`, `set/email`, `set/homenode`, `register` | Edit Profile, SysOp Users | Grid square, home node, email, and registration state must not be recalculated differently by web and telnet. Registration requests should verify email before entering the sysop approval queue when SMTP is configured. |
| Authentication | `user_prefs`, registration state | login flow, `set/password`, `set/mfa`, `unset/mfa` | login flow, Edit Profile MFA, SysOp MFA reset | Passwords are hashed before storage; MFA prompts should describe the required code without exposing implementation details. |
| Access matrix | access records and user prefs | `sysop/access`, `sysop/setaccess` | SysOp Users matrix | Web and telnet capability decisions must call the same access checks. |
| Spots and RBN spots | durable spot tables for ordinary spots; bounded memory for live RBN | `dx`, `show/dx`, `show/mydx`, `show/rbn` | public Spots and Watch views | RBN is not a peer family. It is live feed traffic controlled by global/node settings plus per-user preferences and filters. `show/dx` is durable non-RBN history; `show/rbn` is a bounded current-session summary. |
| Filters | `filter_rules` | `accept/spots`, `reject/spots`, `accept/rbn`, `reject/rbn`, `clear/*`, `show/filter` | public filter controls | Web filtering work should target this table directly. `spots` rules apply to normal spots; `rbn` rules apply first to RBN spots, with `spots` as fallback compatibility. Public web common filters may store a single compound `and` expression to preserve web/telnet consistency. |
| Messaging | message tables | `send`, `msg`, `read`, `reply`, `show/messages`, `show/outbox` | mail/message views | Message counters and read state should stay shared between surfaces. |
| Bulletins | bulletin tables | `show/announce`, `show/chat`, `show/wcy`, `show/wwv`, `show/wx` | public/sysop views | Web posting policy is intentionally narrower than telnet/node ingestion. |
| Peers and links | saved peer records, user records for peer accounts, runtime link state | `sysop/peer*`, `show/links`, protocol commands | SysOp Peers/Links | Saved peer config and peer-login accounts are related but separate records. |
| Protocol health | protocol event/ack/history tables | `show/proto*`, `stat/proto*`, `clear/protohistory` | SysOp Protocol Health | Runtime metrics may be live, but event history should be queryable from the store. |
| Data files | configured paths plus refresh metadata | `load/*`, future `sysop/data *` | Node Settings / Doctor | CTY and WPXLOC should be refreshed as managed data, not packaged as stale source-tree fixtures. |

Commands that only write a `user_prefs` key are not automatically stable. They are stable only when another subsystem reads that key and changes behavior, or when the key is a documented profile/preference field surfaced consistently in web and telnet.

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
- `set/password`
- `set/password <newpass> <confirm-newpass>`
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
- `accept/rbn <expr>`
- `reject/rbn <expr>`
- `clear/spots`
- `clear/rbn`
- `clear/accept <family> [slot]` (`clear/acc`)
- `clear/reject <family> [slot]` (`clear/rej`)
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

## Current Audit Snapshot

As of `1.0.13`, the active telnet registry includes persistent VE7CC/CC11 compatibility commands in addition to the audited grouped command surface:

| Family | Count | Review status |
| --- | ---: | --- |
| `show/*` | 141 | Mixed: many are stable reads; several are compatibility aliases or local preference displays. |
| `set/*` | 85 | Needs the most cleanup; many entries are generic preference writers. |
| `unset/*` | 63 | Same cleanup profile as `set/*`. |
| `stat/*` | 23 | Mostly stable if backed by real DB/runtime summaries. |
| `sysop/*` | 22 | Stable directionally; should remain the primary telnet admin namespace. |
| `load/*` | 14 | Mostly status/reload compatibility; should be replaced over time by explicit `sysop/data *` commands. |
| `accept/*`, `reject/*`, `clear/*` | 16 | Stable where backed by `filter_rules`; RBN is now first-class. |
| `create/*`, `delete/*`, `forward/*`, `get/*` | 6 | Mixed user-data and utility commands; keep only where state is persisted and documented. |

Direct aliases are intentionally small: `version`, `dx`, `users`, `node`, `cluster`, `motd`, `date`, `time`, `ap`, `apropos`, `mail`, `outbox`, and `register`.

Database-backed commands already in good shape:

- profile and registration: `register`, `set/name`, `set/qth`, `set/qra`, `set/email`, `set/password`, `set/mfa`, `show/registered`, `show/station`
- filters: `accept/spots`, `reject/spots`, `accept/rbn`, `reject/rbn`, `clear/spots`, `clear/rbn`, `show/filter`
- access and account administration: `sysop/users`, `sysop/showuser`, `sysop/access`, `sysop/setaccess`, `sysop/password`, `sysop/clearpassword`, `sysop/clearmfa`, `sysop/blocklogin`
- messaging and bulletins: `send`, `msg`, `read`, `reply`, `show/messages`, `show/outbox`, `show/announce`, `show/chat`, `show/wcy`, `show/wwv`, `show/wx`
- peers and protocol health: `sysop/peer*`, `show/links`, `show/proto*`, `stat/proto*`, `clear/protohistory`

Commands that need review before they are promoted as stable:

- legacy protocol toggles: `set/wantpc16`, `set/wantpc9x`, `set/sendpc16`, `set/senddbg`
- partially real route toggle: `set/routepc19`, which is read by routing code and should either be documented as stable or moved under a clearer sysop policy command
- legacy preference-only flags: `set/agwengine`, `set/agwmonitor`, `set/bbs`, `set/believe`, `set/hops`, `set/obscount`, `set/passphrase`, `set/pinginterval`
- duplicate controls: `set/dupann`, `set/dupeph`, `set/dupspots`, `set/dupwcy`, `set/dupwwv`, `clear/dupefile`
- external lookup/show commands: `show/db0sdx`, `show/ik3qar`, `show/wm7d`, `show/dxqsl`, which should be classified as optional integrations with clear unavailable output
- `load/*` commands, which currently report loaded state but are not all true reload operations

The `show/ai3i`, `show/n9jr`, and `show/spout` commands are intentional easter eggs and are excluded from cleanup scoring.

## Compatibility Commands to Hide or Deprecate

The current registry recognizes hundreds of commands. A local scan of `telnet_server.py` shows 370 grouped commands, including 85 `set/*`, 63 `unset/*`, and 141 `show/*` entries. Many `set/*` and `unset/*` commands are generic named-variable wrappers rather than clearly designed pyCluster features.

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

## Cleanup Policy

Command cleanup is continuous rather than tied to an old release sequence:

1. Keep deprecated compatibility commands out of ordinary discovery.
2. Classify commands by status, audience, capability, replacement, and visibility
   when metadata is added or changed.
3. Give deprecated commands a useful replacement message before removal.
4. Remove inert named-variable wrappers that do not back real behavior.
5. Keep peer-family configuration in explicit SysOp workflows; legacy
   `set/<family>` forms are compatibility aliases, not the primary interface.
6. Keep tests for stable behavior and intentional compatibility aliases. Do not
   retain behavior solely to improve a parity count.

## Acceptance Criteria

A command is fit to remain stable when:

- it has a documented purpose in this specification
- it appears in the telnet command reference with accurate syntax
- it is discoverable at the right privilege level
- it has focused tests for success and failure paths
- it changes, reads, or reports real pyCluster state
- it has a clear web-console equivalent when the task is operational/sysop oriented
