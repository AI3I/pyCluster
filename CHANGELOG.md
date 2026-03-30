# Changelog

All notable changes to pyCluster should be recorded here.

## 1.0.3 - 2026-03-30

- `show/qrz` now targets real QRZ XML lookups when QRZ credentials are configured, and the prior local history view has moved to `show/lastspot`
- `show/wm7d` now performs a real WM7D callsign lookup
- the documented in-place upgrade path now explicitly covers `1.0.0` through `1.0.3`
- cluster mail has started moving beyond node-local storage:
  - `PC10` is aligned back to talk/direct-message semantics
  - cluster mail transport now uses `PC28`-`PC33`
  - `msg` and `reply` can queue and route mail by the recipient's configured home node
  - pending mail is flushed when the target peer connects
  - message listings now show delivery state
- top-level `links` now shows the richer direct link status view instead of the older `show/connect` session dump

## 1.0.2 - 2026-03-29

### Added

- DXSpider migration tooling:
  - `deploy/migrate.sh`
  - `scripts/migrate_dxspider.py`
  - DXSpider local-data import support for:
    - users
    - home node
    - MOTD
    - bad-word rules
    - simple outbound peer definitions from `connect/*`
    - exact `badip.local` IP export into pyCluster-managed fail2ban block input
- age-based retention tooling:
  - `scripts/cleanup_retention.py`
  - `pycluster-retention.service`
  - `pycluster-retention.timer`
- logrotate policy for `/var/log/pycluster/authfail.log`

### Changed

- README presentation and support matrix wording
- installation, migration, and operations docs now describe validated platforms and current migration/runtime scope more explicitly
- product-facing defaults and examples were scrubbed of site-specific AI3I deployment data
- sysop and public web UI polish continued, including cleanup controls, footer login/logout actions, and sidebar/runtime presentation
- `deploy/upgrade.sh` now performs the 1.0.0 -> 1.0.1 state upgrade tasks automatically
- protocol-health flapping detection no longer treats routine `PC24` traffic as a flap event, avoiding false flapping status in the sysop console (`#32`)
- public web now exposes bulletin traffic on its own tab, including announce, chat, WX, WCY, and WWV activity (`#24`)
- sysop web `Non-Authenticated` defaults now match the enforced access policy in the access matrix
- sysop user and peer views now surface normalized inbound path and transport details, including source and destination ports (`#30`)
- `show/shortcuts` now presents canonical camelcase-style shorthand boundaries more explicitly, and the one-letter `b` alias is accepted for `bye` (`#22`)
- spot posting can now be rate-limited per user across telnet, public web, and sysop web, with shared defaults and sysop overrides (`#31`)
- sysop web now shows visible `Last Path` columns for local users, blocked users, and system operators, and `Recent Spots` now includes the originating `Node`

## 1.0.1 - 2026-03-28

### Upgrade Note

Existing `1.0.0` installations should be upgraded in place with:

```bash
git pull --ff-only
sudo ./deploy/upgrade.sh
sudo ./deploy/doctor.sh
```

The cumulative upgrader used by `deploy/upgrade.sh` hashes any legacy plaintext passwords still stored in `user_prefs`, seeds `config/strings.toml` if it is missing, preserves compatibility with older configs that predate newer optional sections such as `[qrz]`, and keeps the existing config, data, and logs in place.

### Added

- configurable telnet prompt templates via `node.prompt_template`
- `sysop/setprompt` for runtime prompt template changes

### GitHub Issues

- `#4` install/bootstrap credential visibility
- `#20` default access policy for non-authenticated users
- `#3` peer cleanup and disconnect handling hardening
- `#5` WWV/WCY persistence and related operator syntax cleanup
- `#6` telnet login sanitization and negotiation-byte handling
- `#7` node heartbeat / keepalive behavior for linked peers
- `#8` public web frequency display alignment with telnet formatting
- `#13` `show/wm7d` implementation instead of a status stub

### Changed

- telnet prompts now render from a template instead of a fixed `{node}{suffix}` form
- `show/commands` now returns grouped operator help with family filtering
- solar, moon, and grayline views can use stored QRA/node grid context instead of requiring explicit forwarded latitude/longitude
- public web footer/version text now follows `pycluster.__version__`
- web spot table frequency column is labeled `Frequency` to match the current kHz-style formatting
- install and repair now print the bootstrap `SYSOP` credentials prominently, point at `/root/pycluster-initial-sysop.txt`, and require explicit acknowledgement in interactive installs
- default access fallback now treats non-authenticated users as read-only for spot and announce posting until access is explicitly elevated or overridden
- upgrade runs now hash any legacy plaintext passwords still stored in `user_prefs` and seed `config/strings.toml` when it is missing
- protocol-health views now distinguish current flapping from older flap history instead of treating an old flap score as a permanent alert

### Fixed

- bootstrap `SYSOP` password seeding now stores a hash instead of plaintext (`#4`)
- non-authenticated users no longer inherit permissive default posting access for spots and announces (`#20`)
- blocked users are denied consistently across telnet, sysop web, and public web login paths
- DXSpider-compatible keepalive handling now replies to `PC51` pings correctly, allowing validated linked-peer sessions to survive past the old ~900 second timeout window (`#3`, `#7`)
- login callsign sanitization in the telnet path
- public and sysop web bootstrap access documentation for the initial `SYSOP` account
- telnet login corruption caused by negotiation bytes (`#6`)
- peer heartbeat / disconnect behavior regressions (`#3`, `#7`)
- `show/wm7d` returning gateway-status output instead of lookup behavior (`#13`)
- public web frequency display/version consistency issues (`#8`)

## 2026-03 Deployment and Documentation Hardening

### Added

- System Operator web console with runtime, user, peer, protocol, audit, and security views
- public web login, posting, watch, and profile editing flows
- weekly CTY refresh service and timer
- bootstrap `SYSOP` account creation with one-time note output
- sysop web auth now accepts the bootstrap `SYSOP` operator record consistently
- nginx/TLS deployment helper
- fail2ban filters and jails for pyCluster auth failures
- auth-failure log rotation and imported `badip.local` fail2ban reconciliation

### Changed

- telnet output was cleaned up for readability and 80-column friendliness
- `sysop/*` command surface is explicit and operator-focused
- deploy scripts now support validated Debian-family and EL-family Linux targets
- docs now reflect validated hosts, minimum sizing, and unsupported older platforms
- version sourcing now comes from `pycluster.__version__`

### Fixed

- graceful shutdown with active telnet sessions
- duplicate live spot rendering on multi-link ingest
- CTY gaps such as `TX5EU`
- multiple System Operator UI workflow and clarity issues
- deployment issues around:
  - SELinux
  - Python 3.11+ selection
  - fail2ban startup
  - DB ownership
  - uninstall cleanup
- deploy sync overwriting live config/data/log directories
- protocol flap scoring falsely reacting to normal peer state churn
