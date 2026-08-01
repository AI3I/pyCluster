# Changelog

All notable changes to pyCluster should be recorded here.

## 1.0.12 - 2026-08-01

- Fixed telnet authenticator failure fallback so TOTP is retired only after an email challenge is successfully delivered; unavailable SMTP or delivery failure leaves the working authenticator configuration intact.
- Scoped telnet MFA enrollment, disable, and System Operator reset challenge cleanup to the exact callsign/SSID.
- Made web configuration saves atomic and directed them to `pycluster.local.toml`, preserving the tracked/base configuration and ensuring local overrides remain the effective persisted state.
- Fixed PY topology lease renewal, terminal snapshot pagination, delivery accounting, and bounded rolling rate-window memory.
- Upgrade, repair, and uninstall now stop live writers before taking runtime backups so SQLite/WAL snapshots are consistent; failed maintenance restores services that were active before shutdown. Upgrade and repair then use graceful systemd restarts instead of forced process kills.
- Install no longer disables the host SSH fail2ban jail; the scanner jail is limited to malformed callsigns, and uninstall removes the complete on-demand upgrade watcher.
- nginx setup validates hostnames and restores every configuration file it changed when validation, restart, or certificate provisioning fails.

### Added

- The opt-in, versioned pyCluster-only `PY00` hello negotiates direct-peer capabilities after authenticated PC18 identification; per-link size/rate controls and negotiated `PY99` errors keep private-protocol traffic bounded and isolated from other cluster families.
- Negotiated `PY01` NODEINFO records expose bounded, expiring direct-peer metadata with stable installation identity, monotonic content sequencing, explicit public URLs, provenance, and conflict checks without forwarding records.
- Opt-in `PY02`/`PY10`/`PY03` topology reconciliation adds a persistent known-node catalog, digest-first selective requests, bounded record batches, direct-over-reported precedence, hop limits, source-loop prevention, periodic jittered refresh, expiry pruning, and an authenticated SysOp catalog API.
- Opt-in `PY04`, `PY05`, `PY06`, `PY08`, and `PY09` frames add strictly validated direct-peer health, dataset freshness, aggregate RBN status, access-policy, and clock summaries. Latest values are persisted in peer protocol state and exposed through the authenticated SysOp peers API; secrets, paths, endpoints, logs, users, and spot records are excluded.
- Structured `PY07` notices add sequenced active/cancel records with severity and bounded expiry, while snapshot IDs and ordered pages prevent partial topology inventories from appearing complete.
- The System Operator Node Settings view now includes a dedicated pyCluster Protocol tab for sharing/privacy controls, metadata preview, and network-notice management; Protocol Health retains live state and the Known pyCluster Nodes catalog.

## 1.0.11 - 2026-07-31

### Added

- System Operator Users now includes a Locked view backed by the same account matrix as the main Users and Blocked views.
- Public web authenticator-app MFA enrollment shows both a QR code and a manual setup key.
- System Operator registration requests now call out pending requests older than 24 hours.
- Public web MFA controls now expose email OTP enrollment directly when MFA is disabled.
- An optional fail2ban account-lock action and host-local lock/unlock helper were added for structured auth-failure callsigns.

### Changed

- Direct RBN feed ingestion now batches accepted spots, yields during burst fanout, and caps live RBN aggregation state to reduce freeze and memory-growth risk on busy feeds.
- Live telnet RBN summaries collect reports for ten seconds, combine frequencies within 0.5 kHz, cap quality at `Q:9`, merge CQ zones, and suppress the same call/frequency/mode respot for three minutes.
- Filtered `show/mydx` scans deeper durable spot history before reporting no matches, improving behavior when recent RBN or nonmatching traffic dominates the latest rows.
- Core authentication and the shipped fail2ban core jail now use the same five-failure account-lock threshold.

### Fixed

- Telnet registration requests no longer create local user accounts before SysOp approval.
- Denied or deleted accounts now clear stale local user, registration, and MFA challenge state so reused callsign-SSIDs do not inherit old email/MFA data.
- Telnet idle keepalive prompt refresh no longer adds extra blank lines.
- Locked exact callsign-SSID accounts are rejected before password or MFA prompts.
- Outbound PC92 path sanitization and PC61 spot relay now use detected global interface addresses as a runtime fallback when public IP fields are blank, avoiding `localhost` or private-address protocol payloads on nodes that have not saved explicit public addresses.
- Telnet async DX/RBN spot output starts on a fresh line after idle keepalive prompt refreshes.
- Telnet keepalive writes mark the receiving session's prompt line directly, including callback paths where session-id lookup is unavailable.
- Telnet first-login email verification keeps live DX/RBN spot delivery suppressed while the user is entering the verification code.
- Public web spot visibility treats rows sourced from the RBN source node as RBN traffic so anonymous and non-opted-in users do not see them.
- Public web websocket, spot history, statistics, and leaderboard paths apply the same database-backed RBN preference and filters, including raw PC11/PC61 RBN markers outside the visible comment.
- PC18 handshakes identify the software as pyCluster with its own version instead of presenting a synthetic DXSpider version string; the protocol field remains DX-compatible.
- Outbound `dxspider://` startup no longer sends a duplicate application PC18.
- Outbound `dxspider://` startup records DXSpider's PC18 banner without returning a PC18 that DXSpider intentionally ignores, and sends exactly one startup PC20.
- DXSpider-family links use PC51 ping/reply traffic for steady-state liveness instead of periodic PC20 frames that cause DXSpider to retransmit PC19/PC22 startup configuration.
- Public web and telnet MFA state now stays scoped to the exact logged-in callsign or SSID instead of inheriting or modifying base/sibling MFA settings.
- Fresh callsign-SSIDs without an exact registry record no longer inherit a base account's email merely because global MFA is enabled.
- Failed-password counters and account locks apply to the exact callsign-SSID being authenticated and are cleared consistently after successful login or reset.
- Disabling authenticator MFA with `unset/totp` now turns off the email OTP fallback for that exact account.
- Public web authenticator verification keeps email OTP enabled as fallback and reports that state in the System Operator user record.

## 1.0.10 - 2026-07-14

### Added

- RBN/Skimmer spot handling now accepts documented `-#` Skimmer spotter suffixes.
- `show/rbn [call] [limit]` shows summarized RBN/Skimmer reports for a callsign.
- Optional direct RBN-enabled telnet feed ingestion can log in, send startup commands, and store DX-style Skimmer spots.
- Public web spot filters now persist common filter combinations into database-backed `accept/spots` rules shared with telnet.
- Public web users can request a self-service password reset by verified email address; a successful reset updates the password and clears failed-password account locks.

### Changed

- RBN spots are opt-in for telnet users by default through `set/rbn`; `unset/rbn` disables live RBN delivery.
- `show/dx` now remains a traditional DX spot history and excludes RBN/Skimmer reports. Use `show/rbn` for RBN history and `show/mydx` for filtered personal spot history.
- Telnet self-registration verifies the user's email before creating the sysop review request when SMTP is configured.
- Public web filtering no longer exposes ITU-zone filter controls; CQ zone, continent, band, mode, activity, and spotter filters remain available.
- The public web map seeds the QTH marker from the logged-in user's stored profile grid when no local map override exists.
- Public web failed-password attempts now use the same durable failed-password lock state as telnet and send an account-lock notice when SMTP and a verified email are available.

### Fixed

- Public web RBN/Skimmer spot visibility now honors the user's database-backed `set/rbn` preference and `accept/rbn`/`reject/rbn` filters.
- Telnet `set/password` now requires password confirmation and the bare interactive form prompts without echoing the password.
- Self-registration callsign validation now rejects user-name-shaped values where the first digit appears too late to look like an amateur callsign.
- Telnet command handling no longer force-flushes still-forming RBN batches around every command, preventing partial RBN summaries from splitting into multiple lines or interleaving with `sh/mydx` replies.
- Live RBN aggregation now collapses already-summarized upstream `-#` reports for the same call/frequency/time bucket into a single summary line.
- DXSpider-profile peers receive legacy PC11 spot relay frames even when configured over a normal TCP DSN.
- Telnet keepalive prompts are line-terminated and freshly rendered during idle waits.
- Telnet startup command output is separated from the final login prompt so configured startup displays do not run into the prompt line.
- Telnet self-service MFA commands now apply to the exact logged-in callsign or SSID instead of collapsing every action to the base callsign.
- Outbound PC92 path data sanitizes non-public IPv4 and IPv6 literals when `public_ip_address` is configured.
- Bare-metal nginx setup always writes a `pycluster-sysop.conf` file; when no sysop hostname is configured it is an inert placeholder rather than a public listener.

## 1.0.8 - 2026-05-10

### Added

- User self-service MFA controls in public web and telnet.
- System Operator user controls for block/unblock, account unlock, MFA status, and authenticator enrollment.
- Public web spot filtering enhancements and backend Kp propagation data.
- Console upgrade worker support for using the source checkout path.

### Changed

- Cluster peer records are separated from local users in the System Operator console.
- Node-wide MFA defaults wait for usable user MFA material instead of locking users out before setup.
- Data refresh timers, deployment units, and operations documentation were refreshed.
- Public web and System Operator controls were polished for mobile and foldable layouts.

### Fixed

- SSID users no longer inherit base-call permissions when they have explicit local records.
- Telnet keepalive handling, password prompt echo handling, `announce/full`, `show/muf`, and `show/moon` behavior were tightened.
- Public web registration approval and email verification state now produce authenticated, verified user records.
- Saved peer connect/disconnect flows preserve stored peer definitions and credentials.
- Mobile public web sidebar, footer controls, toast placement, modals, and System Operator tab rows no longer overflow or disappear on narrow screens.
- Upgrade and repair scripts now replace an invalid runtime `strings.toml` with the bundled catalog after backing it up.
- Outbound node-link reconnects now advertise the local PC18 software identity so peers refresh cached pyCluster versions.

## 1.0.7 - 2026-04-29

### Added

- System Operator web controls for QRZ XML credentials used by `show/qrz`
- Saved peer deletion from the System Operator peer editor
- Google Authenticator-compatible TOTP MFA enrollment and login verification
- Satellite pass prediction for `show/satellite <target>` using local TLE/keps data
- Dedicated public-web taxonomy editor in the System Operator web console
- More aggressive telnet scanner `fail2ban` jail for repeated invalid login attempts
- Public web controls for hiding spot popups and the sidebar

### Changed

- Default fresh-install authentication gates are off until a sysop enables them
- System Operator maintenance actions are grouped under the Maintenance node-settings pane
- System Operator terminology and recent-authentication-failure labels are clearer
- SMTP settings now include a direct test-email action from the web console
- System Operator peer health now separates live transport activity from stale inbound protocol state

### Fixed

- Remote `talk` routing now relays direct talk messages over node links
- Local announce, WX, WCY, and WWV posts are delivered to local users and relayed to peers
- Cluster user totals now use explicit remote roster reports instead of protocol frame counters
- Saved inbound peer definitions can be stored without a transport address and remain visible/editable in the System Operator peer table
- pyCluster-profile peers now receive keepalive frames, and one-way active links report as transmit-active/receive-quiet instead of just stale
- Live WWV announcements use the aligned table-style header instead of running into the prompt
- New peer creation clears the whole peer editor, including the peer filter
- Let's Encrypt setup fails early when required certificate input is missing or host web ports are already occupied by a non-nginx service
- Public web spot toasts no longer cover the sidebar
- User deletion removes the full local account footprint instead of leaving stale preferences behind

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

- PC20 keepalive now sent to all connected peers (inbound and outbound), preventing false stale/disconnected state on low-traffic links
- Receiving a PC20 ping from a peer now refreshes its activity timestamp
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
