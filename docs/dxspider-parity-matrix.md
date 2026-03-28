# DXSpider Command Parity Matrix (1.55/1.57)

Generated UTC: 2026-03-10T13:50:56.587344+00:00

## Summary

- Total catalog commands: 271
- Complete: 271
- Partial: 0
- Missing: 0

## Criteria

- `complete`: resolves to an implemented command path and returns concrete behavior.
- `partial`: resolves, but currently generic/limited/privilege-gated/arg-dependent behavior.
- `missing`: unresolved or explicit not-implemented path.

## Missing (0)

| Command | Status | Resolved | Note |
|---|---|---|---|

## Partial (0)

| Command | Status | Resolved | Note |
|---|---|---|---|

## Complete (271)

| Command | Status | Resolved | Note |
|---|---|---|---|
| `accept/announce` | `complete` | `accept/announce` |  |
| `accept/route` | `complete` | `accept/route` |  |
| `accept/spots` | `complete` | `accept/spots` |  |
| `accept/wcy` | `complete` | `accept/wcy` |  |
| `accept/wwv` | `complete` | `accept/wwv` |  |
| `agwrestart` | `complete` | `agwrestart` | real behavior path; privilege-gated |
| `announce` | `complete` | `announce` |  |
| `apropos` | `complete` | `apropos` |  |
| `blank` | `complete` | `blank` |  |
| `bye` | `complete` | `bye` |  |
| `catchup` | `complete` | `catchup` |  |
| `chat` | `complete` | `chat` |  |
| `clear/announce` | `complete` | `clear/announce` |  |
| `clear/dupefile` | `complete` | `clear/dupefile` |  |
| `clear/route` | `complete` | `clear/route` |  |
| `clear/spots` | `complete` | `clear/spots` |  |
| `clear/wcy` | `complete` | `clear/wcy` |  |
| `clear/wwv` | `complete` | `clear/wwv` |  |
| `connect` | `complete` | `connect` |  |
| `create/user` | `complete` | `create/user` |  |
| `dbavail` | `complete` | `dbavail` |  |
| `dbcreate` | `complete` | `dbcreate` | real behavior path; privilege-gated |
| `dbdelkey` | `complete` | `dbdelkey` | real behavior path; privilege-gated |
| `dbexport` | `complete` | `dbexport` | real behavior path; privilege-gated |
| `dbimport` | `complete` | `dbimport` | real behavior path; privilege-gated |
| `dbremove` | `complete` | `dbremove` | real behavior path; privilege-gated |
| `dbshow` | `complete` | `dbshow` |  |
| `dbupdate` | `complete` | `dbupdate` | real behavior path; privilege-gated |
| `debug` | `complete` | `debug` |  |
| `delete/usdb` | `complete` | `delete/usdb` |  |
| `delete/user` | `complete` | `delete/user` |  |
| `demonstrate` | `complete` | `demonstrate` |  |
| `directory` | `complete` | `directory` |  |
| `disconnect` | `complete` | `disconnect` |  |
| `do` | `complete` | `do` |  |
| `dx` | `complete` | `dx` |  |
| `dxqsl_export` | `complete` | `dxqsl_export` |  |
| `dxqsl_import` | `complete` | `dxqsl_import` |  |
| `echo` | `complete` | `echo` |  |
| `export` | `complete` | `export` | real behavior path; privilege-gated |
| `export_users` | `complete` | `export_users` | real behavior path; privilege-gated |
| `forward/latlong` | `complete` | `forward/latlong` |  |
| `forward/opername` | `complete` | `forward/opername` |  |
| `get/keps` | `complete` | `get/keps` |  |
| `help` | `complete` | `help` |  |
| `init` | `complete` | `init` | real behavior path; privilege-gated |
| `join` | `complete` | `join` |  |
| `kill` | `complete` | `kill` | real behavior path; privilege-gated |
| `leave` | `complete` | `leave` |  |
| `links` | `complete` | `links` |  |
| `load/aliases` | `complete` | `load/aliases` |  |
| `load/badmsg` | `complete` | `load/badmsg` |  |
| `load/badwords` | `complete` | `load/badwords` |  |
| `load/bands` | `complete` | `load/bands` |  |
| `load/cmd_cache` | `complete` | `load/cmdcache` |  |
| `load/db` | `complete` | `load/db` |  |
| `load/dxqsl` | `complete` | `load/dxqsl` |  |
| `load/forward` | `complete` | `load/forward` |  |
| `load/hops` | `complete` | `load/hops` |  |
| `load/keps` | `complete` | `load/keps` |  |
| `load/messages` | `complete` | `load/messages` |  |
| `load/prefixes` | `complete` | `load/prefixes` |  |
| `load/swop` | `complete` | `load/swop` |  |
| `load/usdb` | `complete` | `load/usdb` |  |
| `merge` | `complete` | `merge` |  |
| `msg` | `complete` | `msg` |  |
| `pc` | `complete` | `pc` |  |
| `ping` | `complete` | `ping` |  |
| `privilege` | `complete` | `privilege` |  |
| `rcmd` | `complete` | `rcmd` |  |
| `read` | `complete` | `read` |  |
| `reject/announce` | `complete` | `reject/announce` |  |
| `reject/route` | `complete` | `reject/route` |  |
| `reject/spots` | `complete` | `reject/spots` |  |
| `reject/wcy` | `complete` | `reject/wcy` |  |
| `reject/wwv` | `complete` | `reject/wwv` |  |
| `reply` | `complete` | `reply` |  |
| `rinit` | `complete` | `rinit` | real behavior path; privilege-gated |
| `run` | `complete` | `run` |  |
| `save` | `complete` | `save` |  |
| `send` | `complete` | `send` |  |
| `send_config` | `complete` | `send_config` |  |
| `set/address` | `complete` | `set/address` |  |
| `set/announce` | `complete` | `set/announce` |  |
| `set/anntalk` | `complete` | `set/anntalk` |  |
| `set/arcluster` | `complete` | `set/arcluster` |  |
| `set/baddx` | `complete` | `set/baddx` |  |
| `set/badnode` | `complete` | `set/badnode` |  |
| `set/badspotter` | `complete` | `set/badspotter` |  |
| `set/badword` | `complete` | `set/badword` |  |
| `set/bbs` | `complete` | `set/bbs` |  |
| `set/beep` | `complete` | `set/beep` |  |
| `set/buddy` | `complete` | `set/buddy` |  |
| `set/clx` | `complete` | `set/clx` |  |
| `set/debug` | `complete` | `set/debug` |  |
| `set/dx` | `complete` | `set/dx` |  |
| `set/dxcq` | `complete` | `set/dxcq` |  |
| `set/dxgrid` | `complete` | `set/dxgrid` |  |
| `set/dxitu` | `complete` | `set/dxitu` |  |
| `set/dxnet` | `complete` | `set/dxnet` |  |
| `set/echo` | `complete` | `set/echo` |  |
| `set/email` | `complete` | `set/email` |  |
| `set/here` | `complete` | `set/here` |  |
| `set/homebbs` | `complete` | `set/homebbs` |  |
| `set/homenode` | `complete` | `set/homenode` |  |
| `set/hops` | `complete` | `set/hops` |  |
| `set/isolate` | `complete` | `set/isolate` |  |
| `set/language` | `complete` | `set/language` |  |
| `set/local_node` | `complete` | `set/localnode` |  |
| `set/location` | `complete` | `set/location` |  |
| `set/lockout` | `complete` | `set/lockout` |  |
| `set/logininfo` | `complete` | `set/logininfo` |  |
| `set/maxconnect` | `complete` | `set/maxconnect` |  |
| `set/name` | `complete` | `set/name` |  |
| `set/node` | `complete` | `set/node` |  |
| `set/obscount` | `complete` | `set/obscount` |  |
| `set/page` | `complete` | `set/page` |  |
| `set/passphrase` | `complete` | `set/passphrase` |  |
| `set/password` | `complete` | `set/password` |  |
| `set/pinginterval` | `complete` | `set/pinginterval` |  |
| `set/privilege` | `complete` | `set/privilege` |  |
| `set/prompt` | `complete` | `set/prompt` |  |
| `set/qra` | `complete` | `set/qra` |  |
| `set/qth` | `complete` | `set/qth` |  |
| `set/register` | `complete` | `set/register` |  |
| `set/routepc19` | `complete` | `set/routepc19` |  |
| `set/send_dbg` | `complete` | `set/senddbg` |  |
| `set/sendpc16` | `complete` | `set/sendpc16` |  |
| `set/dxspider` | `complete` | `set/dxspider` |  |
| `set/startup` | `complete` | `set/startup` |  |
| `set/sys_location` | `complete` | `set/syslocation` |  |
| `set/sys_qra` | `complete` | `set/sysqra` |  |
| `set/talk` | `complete` | `set/talk` |  |
| `set/usdb` | `complete` | `set/usdb` |  |
| `set/user` | `complete` | `set/user` |  |
| `set/uservar` | `complete` | `set/uservar` |  |
| `set/usstate` | `complete` | `set/usstate` |  |
| `set/var` | `complete` | `set/var` |  |
| `set/wantpc16` | `complete` | `set/wantpc16` |  |
| `set/wantpc9x` | `complete` | `set/wantpc9x` |  |
| `set/wcy` | `complete` | `set/wcy` |  |
| `set/wwv` | `complete` | `set/wwv` |  |
| `set/wx` | `complete` | `set/wx` |  |
| `show/425` | `complete` | `show/425` |  |
| `show/announce` | `complete` | `show/announce` |  |
| `show/baddx` | `complete` | `show/baddx` |  |
| `show/badnode` | `complete` | `show/badnode` |  |
| `show/badspotter` | `complete` | `show/badspotter` |  |
| `show/badword` | `complete` | `show/badword` |  |
| `show/bands` | `complete` | `show/bands` |  |
| `show/buddy` | `complete` | `show/buddy` |  |
| `show/chat` | `complete` | `show/chat` |  |
| `show/cluster` | `complete` | `show/cluster` |  |
| `show/cmd_cache` | `complete` | `show/cmdcache` |  |
| `show/configuration` | `complete` | `show/configuration` |  |
| `show/connect` | `complete` | `show/connect` |  |
| `show/contest` | `complete` | `show/contest` |  |
| `show/date` | `complete` | `show/date` |  |
| `show/db0sdx` | `complete` | `show/db0sdx` |  |
| `show/debug` | `complete` | `show/debug` |  |
| `show/dup_ann` | `complete` | `show/dupann` |  |
| `show/dup_eph` | `complete` | `show/dupeph` |  |
| `show/dup_spots` | `complete` | `show/dupspots` |  |
| `show/dup_wcy` | `complete` | `show/dupwcy` |  |
| `show/dup_wwv` | `complete` | `show/dupwwv` |  |
| `show/dx` | `complete` | `show/dx` |  |
| `show/dxqsl` | `complete` | `show/dxqsl` |  |
| `show/dxstats` | `complete` | `show/dxstats` |  |
| `show/files` | `complete` | `show/files` |  |
| `show/filter` | `complete` | `show/filter` |  |
| `show/grayline` | `complete` | `show/grayline` |  |
| `show/groups` | `complete` | `show/groups` |  |
| `show/heading` | `complete` | `show/heading` |  |
| `show/hfstats` | `complete` | `show/hfstats` |  |
| `show/hftable` | `complete` | `show/hftable` |  |
| `show/hops` | `complete` | `show/hops` |  |
| `show/ik3qar` | `complete` | `show/ik3qar` |  |
| `show/isolate` | `complete` | `show/isolate` |  |
| `show/lockout` | `complete` | `show/lockout` |  |
| `show/log` | `complete` | `show/log` |  |
| `show/moon` | `complete` | `show/moon` |  |
| `show/motd` | `complete` | `show/motd` |  |
| `show/msg_status` | `complete` | `show/msgstatus` |  |
| `show/muf` | `complete` | `show/muf` |  |
| `show/newconfiguration` | `complete` | `show/newconfiguration` |  |
| `show/node` | `complete` | `show/node` |  |
| `show/prefix` | `complete` | `show/prefix` |  |
| `show/program` | `complete` | `show/program` |  |
| `show/qra` | `complete` | `show/qra` |  |
| `show/qrz` | `complete` | `show/qrz` |  |
| `show/rcmd` | `complete` | `show/rcmd` |  |
| `show/registered` | `complete` | `show/registered` |  |
| `show/route` | `complete` | `show/route` |  |
| `show/satellite` | `complete` | `show/satellite` |  |
| `show/startup` | `complete` | `show/startup` |  |
| `show/station` | `complete` | `show/station` |  |
| `show/sun` | `complete` | `show/sun` |  |
| `show/talk` | `complete` | `show/talk` |  |
| `show/time` | `complete` | `show/time` |  |
| `show/usdb` | `complete` | `show/usdb` |  |
| `show/users` | `complete` | `show/users` |  |
| `show/var` | `complete` | `show/var` |  |
| `show/version` | `complete` | `show/version` |  |
| `show/vhfstats` | `complete` | `show/vhfstats` |  |
| `show/vhftable` | `complete` | `show/vhftable` |  |
| `show/wcy` | `complete` | `show/wcy` |  |
| `show/wm7d` | `complete` | `show/wm7d` |  |
| `show/wwv` | `complete` | `show/wwv` |  |
| `show/wx` | `complete` | `show/wx` |  |
| `shu` | `complete` | `shu` |  |
| `shutdown` | `complete` | `shutdown` | real behavior path; privilege-gated |
| `spoof` | `complete` | `spoof` | real behavior path; privilege-gated |
| `stat/channel` | `complete` | `stat/channel` |  |
| `stat/db` | `complete` | `stat/db` |  |
| `stat/msg` | `complete` | `stat/msg` |  |
| `stat/nodeconfig` | `complete` | `stat/nodeconfig` |  |
| `stat/pc19list` | `complete` | `stat/pc19list` |  |
| `stat/route_node` | `complete` | `stat/routenode` |  |
| `stat/route_user` | `complete` | `stat/routeuser` |  |
| `stat/user` | `complete` | `stat/user` |  |
| `stat/userconfig` | `complete` | `stat/userconfig` |  |
| `sysop` | `complete` | `sysop` | real behavior path; privilege-gated |
| `talk` | `complete` | `talk` |  |
| `type` | `complete` | `type` |  |
| `uncatchup` | `complete` | `uncatchup` |  |
| `unset/announce` | `complete` | `unset/announce` |  |
| `unset/anntalk` | `complete` | `unset/anntalk` |  |
| `unset/baddx` | `complete` | `unset/baddx` |  |
| `unset/badnode` | `complete` | `unset/badnode` |  |
| `unset/badspotter` | `complete` | `unset/badspotter` |  |
| `unset/badword` | `complete` | `unset/badword` |  |
| `unset/beep` | `complete` | `unset/beep` |  |
| `unset/buddy` | `complete` | `unset/buddy` |  |
| `unset/debug` | `complete` | `unset/debug` |  |
| `unset/dx` | `complete` | `unset/dx` |  |
| `unset/dxcq` | `complete` | `unset/dxcq` |  |
| `unset/dxgrid` | `complete` | `unset/dxgrid` |  |
| `unset/dxitu` | `complete` | `unset/dxitu` |  |
| `unset/echo` | `complete` | `unset/echo` |  |
| `unset/email` | `complete` | `unset/email` |  |
| `unset/here` | `complete` | `unset/here` |  |
| `unset/hops` | `complete` | `unset/hops` |  |
| `unset/isolate` | `complete` | `unset/isolate` |  |
| `unset/local_node` | `complete` | `unset/localnode` |  |
| `unset/lockout` | `complete` | `unset/lockout` |  |
| `unset/logininfo` | `complete` | `unset/logininfo` |  |
| `unset/passphrase` | `complete` | `unset/passphrase` |  |
| `unset/password` | `complete` | `unset/password` |  |
| `unset/privilege` | `complete` | `unset/privilege` |  |
| `unset/prompt` | `complete` | `unset/prompt` |  |
| `unset/register` | `complete` | `unset/register` |  |
| `unset/routepc19` | `complete` | `unset/routepc19` |  |
| `unset/send_dbg` | `complete` | `unset/senddbg` |  |
| `unset/sendpc16` | `complete` | `unset/sendpc16` |  |
| `unset/startup` | `complete` | `unset/startup` |  |
| `unset/talk` | `complete` | `unset/talk` |  |
| `unset/usstate` | `complete` | `unset/usstate` |  |
| `unset/wantpc16` | `complete` | `unset/wantpc16` |  |
| `unset/wantpc9x` | `complete` | `unset/wantpc9x` |  |
| `unset/wcy` | `complete` | `unset/wcy` |  |
| `unset/wwv` | `complete` | `unset/wwv` |  |
| `unset/wx` | `complete` | `unset/wx` |  |
| `wcy` | `complete` | `wcy` |  |
| `who` | `complete` | `who` |  |
| `wwv` | `complete` | `wwv` |  |
| `wx` | `complete` | `wx` |  |

## Prioritized Next Work
