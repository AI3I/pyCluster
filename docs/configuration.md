# Configuration

Primary config file:

- `config/pycluster.toml`

Optional local override file:

- `config/pycluster.local.toml`

pyCluster loads `config/pycluster.toml` first, then layers `config/pycluster.local.toml` on top when it exists. Put host-specific settings in the local override file so upgrades do not require editing the tracked base config.

## Main Sections

### `[node]`

Identity and presentation for the local cluster node.

Important fields:

- `node_call`
- `node_alias`
- `owner_name`
- `qth`
- `node_locator`
- `motd`
- `support_contact`
- `website_url`
- `public_ip_address`
- `public_ipv6_address`
- `require_password`
- `registration_required`
- `verified_email_required_for_web`
- `verified_email_required_for_telnet`
- `initial_grace_logins`

Auth policy notes:

- `registration_required` is now the primary ordinary-user gate.
- When `registration_required = true`, ordinary human users must have an approved local registration before login. Public and telnet registration requests remain pending until a System Operator approves them.
- When `registration_required = false`, the public account-setup flow activates an account immediately after email verification; it does not create a pending System Operator request.
- When `verified_email_required_for_web = true`, ordinary web login requires a verified account email. Email verification authenticates the account independently of registration approval.
- When `verified_email_required_for_telnet = true`, ordinary telnet login requires a verified email address. A new caller is prompted only for the email needed for authentication and its verification code; the profile registration interview runs only after the user explicitly enters `REGISTER`.
- `initial_grace_logins` controls how many failed or skipped telnet verification attempts are allowed before the pending account is locked.
- `require_password` is now a narrower legacy telnet-password toggle. For ordinary human users, the stronger registration policy effectively implies passworded access.
- `public_ip_address` is optional IPv4. `public_ipv6_address` is optional IPv6. When set, outbound PC92 path data replaces private, loopback, link-local, `localhost`, or otherwise non-public IP literals with the same-family configured public address before advertising them to peers. Locally generated PC61 spot and PC93 chat/bulletin relay frames use the same public-address selection. If either field is blank, pyCluster detects global interface addresses at runtime and uses them as protocol fallbacks. Existing configs that stored an IPv6 address in `public_ip_address` are accepted and migrated in memory.

### `[telnet]`

Telnet listener settings.

Important fields:

- `host`
- `port` - single telnet listener port, default `7300`
- `ports` - optional list of listener ports; when non-empty, this takes precedence over `port`
- `max_clients`
- `idle_timeout_seconds`
- `keepalive_interval_seconds`
- `max_line_length`

`idle_timeout_seconds` controls authentication/input timeout behavior. `keepalive_interval_seconds` independently controls post-login application keepalives; it defaults to 300 seconds and emits a visible prompt while continuing to wait for input. Telnet sockets also enable operating-system TCP keepalive.

Listener address forms on supported Linux hosts:

- `127.0.0.1` or `0.0.0.0` - IPv4 loopback or wildcard
- `::1` or `::` - IPv6 loopback or wildcard; `::` is IPv6-only
- an empty string (`host = ""`) - request separate IPv4 and IPv6 wildcard sockets
- a specific IPv4 or IPv6 address - bind only that interface address

The same address forms apply to `[web].host` and `[public_web].host`. Wildcard and dual-stack bindings must be restricted with both IPv4 and IPv6 firewall policy.

### `[web]`

System Operator web console listener.

Important fields:

- `host`
- `port`
- `admin_token`

Note:
- the sysop console uses callsign/password auth
- keep it on `127.0.0.1` behind a reverse proxy unless you have a reason not to
- a default install does exactly that; it does not expose the sysop web listener publicly by itself
- `deploy/setup-nginx.sh --sysop-host <host>` publishes it through nginx on the pyCluster host
- an external reverse proxy can reach it over a trusted LAN after you explicitly change `host`; restrict that listener to the proxy or management network and use HTTPS at the proxy

### `[public_web]`

Public web UI and API.

Important fields:

- `enabled`
- `host`
- `port`
- `static_dir`
- `cty_dat_path`
- `wpxloc_raw_path`

Note:
- the public web listener is local by default at `127.0.0.1:8081`
- expose it through nginx or another reverse proxy when you want public access
- `deploy/install.sh` can now call `deploy/setup-nginx.sh` interactively during first install
- the supported nginx helper writes pyCluster-owned server blocks under `/etc/nginx/conf.d`
- a reverse proxy on another host requires this listener to bind to the VM's LAN address or `0.0.0.0`, plus a firewall rule allowing the proxy host to reach it

### `[store]`

Persistent SQLite storage.

Important field:

- `sqlite_path`

### `[smtp]`

Node-wide email delivery settings used for SMTP-backed verification and email MFA.

Important fields:

- `host`
- `port`
- `username`
- `password`
- `from_addr`
- `from_name`
- `starttls`
- `use_ssl` - enable this for implicit TLS/SMTPS, typically port `465`
- `timeout_seconds`

### `[mfa]`

Node-wide MFA policy. Authenticator-app/TOTP challenges use per-user secrets. Email OTP challenges use the `[smtp]` settings and require a valid user email address. Telnet users can enroll TOTP with `set/totp`; enrollment keeps email OTP as fallback for the exact logged-in callsign or SSID. After three failed authenticator-code attempts pyCluster transitions to email OTP only after delivering a challenge successfully. Missing email, unavailable SMTP, or delivery failure preserves the TOTP secret for operator recovery.

Important fields:

- `enabled`
- `require_for_sysop`
- `require_for_users`
- `issuer`
- `otp_ttl_seconds`
- `otp_length`
- `max_attempts`
- `resend_cooldown_seconds`

### `[satellite]`

Local satellite pass-prediction settings for telnet `show/satellite <target>`.

Important fields:

- `keps_path`
- `prediction_hours`
- `pass_step_seconds`
- `min_elevation_deg`

Notes:
- `keps_path` points at a local TLE/keps text file, for example `./data/keps.txt`
- pass prediction uses the caller's QRA/grid square or `forward/latlong` coordinates
- `show/satellite` without a target still shows recent satellite-tagged DX spots

### `[rbn]`

Optional direct RBN/Skimmer telnet feed ingestion. Leave it disabled unless you have a specific RBN-enabled cluster or feed to consume.

Important fields:

- `enabled`
- `host`
- `port`
- `callsign`
- `password`
- `source_node`
- `startup_commands`
- `reconnect_seconds`

Notes:
- `startup_commands` can enable Skimmer traffic on feeds that require it, for example `["set/skimmer"]`
- `port` keeps backward-compatible single-feed behavior. Set `ports = [7000, 7001]` to connect to multiple feeds at the same host.
- `feeds` gives each feed a stable label and can replace `host`/`ports`, for example `feeds = [{ name = "CW/RTTY", host = "telnet.reversebeacon.net", port = 7000 }, { name = "FT8", host = "telnet.reversebeacon.net", port = 7001 }]`.
- The public Reverse Beacon Network relays are `telnet.reversebeacon.net:7000` for CW/RTTY spots and `telnet.reversebeacon.net:7001` for FT8 spots.
- The public RBN relays are high-throughput raw feeds and do not provide cluster-side filtering; use pyCluster filters and user preferences after ingestion.
- RBN feed spots are live traffic. They are filtered and delivered to eligible telnet, web, and peer sessions but are not stored in SQLite or included in historical statistics.
- The public web service receives a best-effort local RBN stream and keeps at most 2,000 reports in memory. Restarting either service clears that display window.
- telnet users do not receive live RBN spots by default; users opt in with `set/rbn` and opt out with `unset/rbn`.
- cluster-peer records and the SysOp access matrix still control whether a peer account may ingest or relay RBN traffic.
- `show/rbn` displays summarized RBN history, while `show/dx` remains the traditional human-posted DX history.
- `accept/rbn` and `reject/rbn` are first-class RBN filter-family commands. `accept/rbn 1 call N9JR` allows only matching RBN/Skimmer spots, while ordinary spot filters remain in the `spots` family.
- use `config/pycluster.local.toml` for host-specific feed credentials

### `[py_protocol]`

Decentralized metadata exchange between authenticated pyCluster peers. It is enabled by default, never requires a central registry or phone-home service, and never sends `PY` traffic to other cluster families. System Operators can disable the protocol or any sharing category.

On a capable pyCluster-to-pyCluster link, negotiated PY families are preferred for the features they implement. PC remains the compatibility fallback and continues to carry operational traffic, such as spots and announcements, for which no PY replacement has been negotiated.

Important fields:

- `enabled` - permits `PY00` negotiation after the remote peer identifies as pyCluster; defaults to `true`
- `defaults_version` - lifecycle-managed marker that applies new installation defaults once without overwriting later System Operator choices
- `public_web_url` - optional externally reachable public-node URL; pyCluster does not infer this from bind addresses or the general project website field
- `share_node_info`
- `share_public_web_url`, `share_locator`, `share_qth`, and `share_sysop_contact` - field-level NODEINFO privacy controls; all default to enabled and can be disabled independently
- `share_topology` - enables bounded `PY02` digest, `PY10` selective request, and `PY03` record reconciliation
- `share_health` - enables direct `PY04` node, service, and link-health summaries
- `share_datasets` - enables direct `PY05` CTY.DAT, wpxloc.raw, and KEPS freshness summaries
- `share_rbn_status` - enables direct `PY06` RBN mode, connection, activity-rate, and queue summaries
- `share_policy` - enables direct `PY08` registration, verification, MFA, and public-access booleans
- `share_clock` - enables direct `PY09` UTC, uptime, and boot-time summaries
- `share_notices` - enables structured `PY07` operator notices
- `notice_severity`, `notice_message`, and `notice_expires_epoch` - dedicated notice content and explicit expiry; an empty message creates an inactive/cancel record
- `max_hops`
- `max_records_per_frame`
- `max_frame_bytes`
- `max_bytes_per_minute`
- `refresh_seconds`
- `record_ttl_seconds`

Safety behavior:

- `PY00` is the only pre-negotiation bootstrap frame.
- Every other PY family requires a capability advertised by both peers.
- PY traffic is rejected on DXSpider, DXNet, AR-Cluster, CLX, unknown, or unauthenticated links.
- Frame-size and per-minute byte limits apply independently in each direction and reset on reconnect.
- `share_node_info` enables direct `PY01` records; `share_topology` independently enables the persistent known-node catalog and anti-entropy exchange. Both default to enabled and can be disabled independently.
- Topology exchanges send digests before details, request only missing or newer records, enforce record/frame/hop limits, avoid returning learned records to their source peer, and expire stale reports.
- The authenticated SysOp endpoint `GET /api/py-nodes` returns this node's current local catalog and provenance.
- Health, datasets, RBN status, policy, and clock are direct-peer summaries refreshed at a bounded interval and persisted in peer protocol state. Each requires its explicit `share_*` setting and bilateral capability negotiation.
- `PY06` shares named modes and aggregate activity only; feed endpoints, ports, passwords, startup commands, and individual RBN spots remain local.
- `PY07` notices are limited to 240 characters and 30 days, carry an explicit cancellation state, and are not derived from the MOTD.
- The SysOp Node Settings > pyCluster Protocol page edits sharing policy and notices and previews shareable NODEINFO values. Protocol Health lists the durable known-node catalog and live protocol state.
- PY metadata never infers private/internal addresses and rejects private, loopback, or local-only literal public URLs. It must not include secrets, users, mail, registration records, logs, full RBN spot streams, or remote configuration mutations.

Use `config/pycluster.local.toml` to enable this per host without modifying the tracked default configuration. System Console configuration saves atomically replace this local override rather than rewriting the base file, so the saved effective configuration and the next-start configuration are identical.

## Example Paths

Default deployed layout:

- config: `/home/pycluster/pyCluster/config/pycluster.toml`
- local override: `/home/pycluster/pyCluster/config/pycluster.local.toml`
- database: `/home/pycluster/pyCluster/data/pycluster.db`
- CTY data: `/home/pycluster/pyCluster/data/cty.dat`
- wpxloc.raw data: `/home/pycluster/pyCluster/data/wpxloc.raw` when you use the standard refresh path
- satellite keps/TLE data: `/home/pycluster/pyCluster/data/keps.txt`

The systemd services run from `/home/pycluster/pyCluster` and load the runtime files above. Editing `/usr/src/pyCluster/config/pycluster.toml` changes the source checkout used for upgrades, not the active installation. Confirm the unit paths with `systemctl cat pycluster.service pyclusterweb.service` when troubleshooting a nonstandard deployment.

## Operational Advice

- keep web listeners local and publish them through a reverse proxy
- use `config/pycluster.local.toml` for hostnames, SMTP credentials, QRZ credentials, and any other host-local settings you do not want overwritten during repo updates
- use realistic `max_clients` values for your hardware
- back up the base config, local override, and SQLite DB together
- do not hand-edit the live CTY file unless you need an emergency local override
- keep SMTP credentials in `config/pycluster.local.toml`, not the tracked base config
- treat `pycluster.local.toml` as generated runtime configuration when using System Console settings; back it up with the base config and database
- keep local satellite keps data under `data/` and point `[satellite].keps_path` at that file
- the installed `pycluster-data-refresh.timer` validates and refreshes CTY, WPXLOC, and Keps every six hours; `deploy/doctor.sh` reports the configured Keps path and file age
- CTY data is used for enrichment and review cues such as suspicious spot-prefix flags in the sysop web UI; it is not treated as a complete worldwide legal callsign authority
- ordinary user access should be managed through the registration and verified-email policy, not by relying only on the older `require_password` toggle


## Dataset Status

- The System Operator Console and telnet `show/configuration` report `CTY.DAT` and `wpxloc.raw` status, path, and version/date when detectable.
- The left navigation in the System Operator Console also shows compact pills for the currently loaded country datasets.
- Suspicious spot-prefix review uses CTY and `wpxloc.raw` as operational cues. If country data is missing or stale, pyCluster reports an advisory state instead of hard-flagging calls as suspicious solely because the prefix could not be recognized.
