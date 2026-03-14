# Telnet Command Reference

This page documents the practical telnet command surface in pyCluster.

It is based on the implemented command registry, not just the historical DXSpider command catalog.

## Command Model

pyCluster uses these main command families:

- `show/*` for visibility
- `set/*` for preferences and control
- `unset/*` to reverse settings
- `accept/*`, `reject/*`, `clear/*` for filters
- `load/*` for cache and data reload helpers
- `stat/*` for compact summaries
- `sysop/*` for privileged operator actions

It also supports direct commands such as:

- `help`
- `ping`
- `bye`
- `connect`
- `disconnect`
- `links`
- `talk`
- `announce`
- `send`
- `msg`
- `read`
- `reply`

## Discovery and Session

| Command | Purpose |
|---|---|
| `help` | Show the main telnet help screen with examples and operator-aware sections. |
| `show/commands [term]` | List available commands, optionally filtered by text. |
| `show/shortcuts [term]` | Show useful abbreviations and direct aliases. |
| `show/apropos <term>` | Search commands by keyword. |
| `ping` | Quick liveness check. |
| `bye` | End the current session. |
| `show/version` | Show program/version information. |
| `show/program` | Show a short program/status summary. |
| `show/date` | Show the current date. |
| `show/time` | Show the current time. |
| `show/uptime` | Show node uptime. |
| `show/motd` | Show the current MOTD. |

## Spot Viewing and Cluster Visibility

| Command | Purpose |
|---|---|
| `show/dx` or `sh/dx` | Show recent DX spots. |
| `show/node [call]` | Show local node state or stored node/user routing info. |
| `show/cluster` | Show a compact cluster summary. |
| `show/users` | Show currently connected users and session details. |
| `show/connect` | Show direct node-link session counters and status. |
| `show/links` | Show direct peer links, family, state, traffic, and loop-drop visibility. |
| `show/route` | Show route-related information. |
| `show/hops` | Show hop-related routing state. |
| `show/prefix <prefix>` | Show local spot counts for a prefix. |
| `show/qrz <call>` | Show the most recent known spot summary for a callsign. |
| `show/qra <call>` | Show stored QRA/grid for a callsign when known. |
| `show/bands` | Show band information. |
| `show/dxstats` | Show overall DX spot statistics. |
| `show/hfstats` | Show HF-oriented spot statistics. |
| `show/vhfstats` | Show VHF-oriented spot statistics. |
| `show/hftable` | Band-table style view for HF ranges. |
| `show/vhftable` | Band-table style view for VHF/UHF ranges. |

Common `sh/dx` examples:

```text
sh/dx 10
sh/dx K3AJ exact
sh/dx by WW5L
sh/dx on 40m
sh/dx on 7000/7300
sh/dx info RTTY
sh/dx day 2
```

## User Profile and Session Preferences

| Command | Purpose |
|---|---|
| `set/name <text>` | Set display name. |
| `set/qth <text>` | Set location/QTH. |
| `set/qra <text>` | Set QRA or grid-style field used by this node. |
| `set/location <text>` | Set location text alias. |
| `set/address <text>` | Set address/contact text. |
| `set/email <addr>` | Set email address. |
| `unset/email` | Clear email address. |
| `set/homenode <node>` | Set home node. |
| `set/homebbs <bbs>` | Set home BBS. |
| `set/node <node>` | Set home-routing node alias. |
| `set/password <newpass>` | Set or change the local password. |
| `unset/password` | Clear the stored local password. |
| `set/passphrase <text>` | Set a passphrase field. |
| `unset/passphrase` | Clear passphrase field. |
| `set/page <n>` | Set pagination length. |
| `set/language <code>` | Set language preference. |
| `set/echo` / `unset/echo` | Enable or disable echo preference. |
| `set/here` / `unset/here` | Enable or disable `here` preference. |
| `set/beep` / `unset/beep` | Enable or disable beep preference. |
| `set/nowrap` / `unset/nowrap` | Disable or restore the default wrapped output style. |
| `set/logininfo` / `unset/logininfo` | Control login-info display preference. |
| `set/maxconnect <n>` | Set the per-callsign connection cap. |
| `set/startup ...` | Add a startup command for login-time replay. |
| `unset/startup ...` | Remove a startup command. |
| `show/startup` | Show configured startup commands. |
| `show/station [call]` | Show stored station/profile details and USDB fields. |
| `show/registered [call]` | Show registry information for users. |
| `show/configuration` | Show node configuration summary. |
| `show/newconfiguration` | Alias to the current configuration view. |

## User Variables, USDB, and Buddy Lists

| Command | Purpose |
|---|---|
| `set/usdb <field> <value>` | Set a structured USDB field. |
| `show/usdb [call]` | Show USDB entries. |
| `delete/usdb <field>` | Remove a USDB field. |
| `set/var <name> <value>` | Set a user variable. |
| `show/var [name]` | Show user variables. |
| `unset/var <name>` | Clear a user variable. |
| `set/uservar <name> <value>` | Set a managed namespaced user variable. |
| `unset/uservar <name>` | Clear a managed namespaced user variable. |
| `set/buddy <call>` | Add a buddy entry. |
| `unset/buddy <call>` | Remove a buddy entry. |
| `show/buddy` | Show the current buddy list. |

## Filters

### Accept/Reject Filters

| Command | Purpose |
|---|---|
| `accept/spots <expr>` | Add an accept rule for spots. |
| `reject/spots <expr>` | Add a reject rule for spots. |
| `clear/spots` | Clear spot filter rules. |
| `accept/rbn <expr>` | Add an RBN-focused accept rule for spot traffic. |
| `reject/rbn <expr>` | Add an RBN-focused reject rule for spot traffic. |
| `clear/rbn` | Clear RBN-focused filter rules. |
| `accept/announce <expr>` | Add an accept rule for announce traffic. |
| `reject/announce <expr>` | Add a reject rule for announce traffic. |
| `clear/announce` | Clear announce filter rules. |
| `accept/route <expr>` | Add an accept rule for route traffic. |
| `reject/route <expr>` | Add a reject rule for route traffic. |
| `clear/route` | Clear route filter rules. |
| `accept/wcy <expr>` | Add an accept rule for WCY traffic. |
| `reject/wcy <expr>` | Add a reject rule for WCY traffic. |
| `clear/wcy` | Clear WCY filter rules. |
| `accept/wwv <expr>` | Add an accept rule for WWV traffic. |
| `reject/wwv <expr>` | Add a reject rule for WWV traffic. |
| `clear/wwv` | Clear WWV filter rules. |
| `accept/wx <expr>` | Add an accept rule for WX traffic. |
| `reject/wx <expr>` | Add a reject rule for WX traffic. |
| `clear/wx` | Clear WX filter rules. |
| `show/filter` | Show current filter state and user filter settings. |

### Global Bad Rules

| Command | Purpose |
|---|---|
| `set/baddx <expr>` | Add a blocked DX rule. |
| `unset/baddx <expr>` | Remove a blocked DX rule. |
| `show/baddx` | Compatibility-recognized bad DX listing path. |
| `set/badnode <expr>` | Add a blocked node rule. |
| `unset/badnode <expr>` | Remove a blocked node rule. |
| `set/badspotter <expr>` | Add a blocked spotter rule. |
| `unset/badspotter <expr>` | Remove a blocked spotter rule. |
| `set/badword <expr>` | Add a blocked word rule. |
| `unset/badword <expr>` | Remove a blocked word rule. |

## Traffic and Messaging

| Command | Purpose |
|---|---|
| `announce <text>` | Send an announcement. |
| `talk <target> <text>` | Send direct talk/chat traffic. |
| `send <call> <text>` | Send a mailbox-style message. |
| `msg <call> <text>` | Alias-style message send path. |
| `read <id>` | Read a stored message. |
| `reply <id> <text>` | Reply to a stored message. |
| `show/messages` | Show stored messages. |
| `show/mail` | Alias to the message list. |
| `show/msgstatus` | Show message count/status summary. |
| `show/announce` | Show stored announcement traffic. |
| `show/chat` | Show stored chat/talk traffic. |
| `show/wcy` | Show stored WCY traffic. |
| `show/wwv` | Show stored WWV traffic. |
| `show/wx` | Show stored WX traffic. |

## Link and Peer Control

| Command | Purpose |
|---|---|
| `connect <peer> <dsn>` | Start a peer connection. |
| `disconnect <peer>` | Disconnect a live peer. |
| `links` | Direct alias to link visibility. |
| `show/capabilities` | Show link/protocol capability summary. |
| `set/dxspider [peer]` | Set local or peer profile behavior to DXSpider style. |
| `set/dxnet [peer]` | Set local or peer profile behavior to DxNet style. |
| `set/arcluster [peer]` | Set local or peer profile behavior to AR-Cluster style. |
| `set/clx [peer]` | Set local or peer profile behavior to CLX style. |
| `unset/dxnet` | Restore local profile to DXSpider style. |
| `unset/arcluster` | Restore local profile to DXSpider style. |
| `unset/clx` | Restore local profile to DXSpider style. |

## Relay, Ingest, and Control Flags

| Command | Purpose |
|---|---|
| `set/relay ...` | Set relay policy. |
| `unset/relay ...` | Clear relay policy. |
| `show/relay` | Show relay policy summary. |
| `set/relaypeer ...` | Set relay policy for a specific peer. |
| `unset/relaypeer ...` | Clear per-peer relay policy. |
| `show/relaypeer` | Show per-peer relay policy. |
| `set/ingestpeer ...` | Set ingest policy for a specific peer. |
| `unset/ingestpeer ...` | Clear per-peer ingest policy. |
| `show/ingestpeer` | Show per-peer ingest policy. |
| `set/control` | Enable control mode/state. |
| `unset/control` | Disable control mode/state. |
| `show/control` | Show system control state and recent control activity. |

## Protocol Visibility and Control

| Command | Purpose |
|---|---|
| `show/proto` | Show protocol summary. |
| `show/protohistory` | Show protocol history events. |
| `show/protoevents` | Show protocol event detail. |
| `show/protoalerts` | Show current protocol alerts. |
| `show/protoacks` | Show protocol acknowledgements. |
| `show/protoack` | Alias to protocol acknowledgements. |
| `show/prack` | Alias to protocol acknowledgements. |
| `show/protoconfig` | Show protocol thresholds/configuration. |
| `show/protothresholds` | Alias to protocol threshold view. |
| `set/protothreshold <key> <value>` | Set a protocol threshold. |
| `set/protothresholds <key> <value>` | Alias to set protocol threshold. |
| `unset/protothreshold <key>` | Clear a protocol threshold. |
| `unset/protothresholds <key>` | Alias to clear protocol threshold. |
| `set/protoack ...` | Acknowledge protocol alerts. |
| `set/prack ...` | Alias to acknowledge protocol alerts. |
| `unset/protoack ...` | Remove protocol alert acknowledgement. |
| `unset/prack ...` | Alias to remove protocol alert acknowledgement. |
| `clear/protohistory` | Clear stored protocol history. |
| `clear/prhist` | Alias to clear protocol history. |
| `clear/prothist` | Alias to clear protocol history. |

## Load and Maintenance Helpers

| Command | Purpose |
|---|---|
| `load/aliases` | Reload alias data. |
| `load/badmsg` | Reload bad-message rules. |
| `load/badwords` | Reload bad-word rules. |
| `load/bands` | Reload band data. |
| `load/cmdcache` | Reload command cache. |
| `load/db` | Reload database-related descriptors or summaries. |
| `load/dxqsl` | Reload DX QSL data. |
| `load/forward` | Reload forward-related state. |
| `load/hops` | Reload hop data. |
| `load/keps` | Reload KEPs/satellite data. |
| `load/messages` | Reload message state. |
| `load/prefixes` | Reload prefix data. |
| `load/swop` | Compatibility load helper. |
| `load/usdb` | Reload USDB data. |
| `clear/dupefile` | Clear duplicate-file tracking. |
| `show/files` | Show useful file or storage state. |
| `show/log [category] [limit]` | Show recent log events. |

## Statistics

| Command | Purpose |
|---|---|
| `stat/spot` or `stat/spots` | Show spot statistics. |
| `stat/user` or `stat/users` | Show user/session statistics. |
| `stat/db` | Show database summary. |
| `stat/msg` | Show message summary. |
| `stat/route` | Show route summary. |
| `stat/proto` | Show protocol summary. |
| `stat/protohistory` | Show protocol-history summary. |
| `stat/protoevents` | Show protocol-event summary. |
| `stat/protoalerts` | Show protocol-alert summary. |
| `stat/protoacks` | Show protocol-ack summary. |
| `stat/protoack` | Alias to protocol-ack summary. |
| `stat/prack` | Alias to protocol-ack summary. |
| `stat/wwv` | Show WWV statistics. |
| `stat/wcy` | Show WCY statistics. |
| `stat/queue` | Show queue summary. |
| `stat/channel` | Show channel summary. |
| `stat/nodeconfig` | Show node-configuration summary. |
| `stat/pc19list` | Show PC19 routing summary. |
| `stat/routenode` | Show route-node summary. |
| `stat/routeuser` | Show route-user summary. |
| `stat/userconfig` | Show user-configuration summary. |

## Environment and Informational Views

| Command | Purpose |
|---|---|
| `show/sun` | Show sun status. |
| `show/grayline` | Show grayline status. |
| `show/moon` | Show moon status. |
| `show/muf` | Show MUF estimate. |
| `show/contest` | Show contest-oriented information. |
| `show/satellite` | Show satellite-oriented information. |
| `show/talk` | Show talk/chat status. |
| `show/debug` | Show debug state. |
| `show/isolate` | Show isolate state. |
| `show/lockout` | Show lockout state. |
| `show/groups` | Show groups state. |
| `show/rcmd` | Show remote-command state. |
| `show/policy` | Show policy summary. |
| `show/policydrop` | Show policy-drop counters and reasons. |
| `show/mydx` | Show personal DX summary. |
| `show/dxcc` | Show DXCC/entity-style information. |
| `show/notimpl` | Show explicitly not-implemented compatibility paths. |

## Operator Commands (`sysop/*`)

These commands require sysop privilege and are hidden from ordinary command listings.

| Command | Purpose |
|---|---|
| `sysop/password <call> <newpass>` | Set another user’s password. |
| `sysop/clearpassword <call>` | Clear another user’s password. |
| `sysop/user <call> <field> <value>` | Update a registry field for a user or node-classified record. |
| `sysop/deleteuser <call>` | Delete a user record. |
| `sysop/privilege <call> <level>` | Set privilege level. |
| `sysop/homenode <call> <node>` | Set another user’s home node. |
| `sysop/blocklogin <call> <on\|off>` | Block or unblock login for a callsign and its SSIDs/base call. |
| `sysop/showuser <call>` | Show a focused user record view. |
| `sysop/users` | Show local users. |
| `sysop/sysops` | Show system operators. |
| `sysop/access <call>` | Show a per-user channel/capability access matrix. |
| `sysop/setaccess <call> <channel\|all> <capability\|all> <on\|off>` | Change user access policy. |
| `sysop/audit [category] [limit]` | Show recent operator audit events. |
| `sysop/services` | Show service-level state exposed by the core app. |
| `sysop/restart <telnet\|sysopweb\|all>` | Restart telnet and/or sysop-web listeners inside the running app. |

## Practical Starting Set

If you are onboarding a new user:

```text
help
show/commands
sh/dx 10
set/name John Lewis
set/qth Western Pennsylvania
set/qra FN00FS
set/password mynewpass
show/station
show/node
```

If you are onboarding a sysop:

```text
help
show/commands
sysop/users
sysop/showuser AI3I
sysop/access AI3I
sysop/setaccess AI3I web login on
show/links
show/proto
sysop/audit
sysop/services
```

## Compatibility Note

pyCluster still recognizes a larger DXSpider-style command universe than this page lists. The purpose of this reference is to document the meaningful implemented surface, not every historical compatibility token or legacy alias.
