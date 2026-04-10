# Changelog

All notable changes to pyCluster should be recorded here.

## 1.0.6 - 2026-04-10

### Added

- Geomagnetic data parsing (`WcyReading`, `WwvReading`) extracted into dedicated `geomag` module
- User registration state management (`registration` module) with email validation and state normalization
- In-place upgrade manager (`upgrade_manager`) with systemd path/service units for zero-touch upgrades via `deploy/systemd/pycluster-upgrade.{path,service}`
- Bundled `CTY.DAT` (VER20260404) and `wpxloc.raw` fixtures updated so fresh installs start with current country data

### Changed

- Deploy tooling (`install.sh`, `upgrade.sh`, `repair.sh`, `setup-nginx.sh`, `doctor.sh`, `lib.sh`) updated for 1.0.6 upgrade paths
- `config/pycluster.toml` and `config/strings.toml` refreshed
- Documentation updated across configuration, installation, node-linking, operations, public-web, sysop-web, and user-manual pages
- README updated

### Fixed

- Various protocol, transport, web-admin, and telnet command improvements carried forward from staging

## 1.0.5 - 2026-04-04

### Changed

- cleaned up the telnet command surface so operator responses are more readable and more consistent across `show/*`, `set/*`, `unset/*`, mail, route, protocol, and sysop command families
- moved a large share of operator-facing telnet text and selected operational log strings into `config/strings.toml` so wording tweaks no longer require code edits or restarts
- public web 24-hour spot stats, history, and leaderboard views now use real time-window queries instead of capped recent-spot snapshots
- the System Operator web console now shows country-data status more clearly, including left-nav pills for loaded `CTY.DAT` and `wpxloc.raw` version/date metadata
- deploy tooling now treats country-data refresh as `CTY.DAT` plus `wpxloc.raw`, and `deploy/doctor.sh` checks the public stats endpoint on the correct listener
- upgrades and deploys are now documented around `config/pycluster.local.toml` so host-local settings stay out of the tracked base config

### Added

- `wpxloc.raw` parsing and fallback lookup support for heading, web spot enrichment, and suspicious-prefix review cues
- email OTP MFA recovery paths in both the System Console and telnet via `sysop/clearmfa <call>`
- stale-user cleanup controls in the System Operator web console
- richer cluster-mail observability in telnet and the System Operator web console

### Fixed

- `set/name`, `set/qth`, `set/qra`, `set/location`, `set/home`, and related `show/*` commands now persist and read back consistently
- `set/location` now takes precedence over `set/qra`, while `set/qra` backfills location only when location is unset
- `show/heading`, `who`, `show/links`, `show/route`, and related peer/operator views now report more accurate live state
- telnet login handling no longer misbehaves when negotiation bytes are present before the callsign
- public web frequency formatting and 24-hour summary counts now match real backend data better
- live spot ingest now uses permissive plausibility checks instead of an over-strict homemade world callsign validator, while suspicious cases are flagged for review instead of being dropped
- cluster mail routing handles offline peers and undeliverable paths more cleanly, with clearer operator readback
- `show/wm7d` CQ-zone handling for calls like `N9JR` now prefers better lookup data instead of stale prefix-only assumptions

## 1.0.4 - 2026-03-30

- fixed the cumulative upgrade path so older `1.0.0` databases with the real `user_prefs(pref_key, pref_value)` schema now upgrade cleanly through `deploy/upgrade.sh`
- added regression coverage for the upgrader against the legacy `1.0.0` config/database shape

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
