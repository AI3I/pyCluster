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
- When `registration_required = true`, ordinary human users must have a local record before account activation.
- When `verified_email_required_for_web = true`, ordinary web login requires a verified email address. If SMTP is configured, unverified users receive an email code challenge instead of a hard denial.
- When `verified_email_required_for_telnet = true`, ordinary telnet login requires a verified email address. New telnet users can complete first-login profile/password setup before the verification gate is enforced, and unverified users are driven through telnet email verification before normal commands continue.
- `initial_grace_logins` controls how many failed or skipped telnet verification attempts are allowed before the pending account is locked.
- `require_password` is now a narrower legacy telnet-password toggle. For ordinary human users, the stronger registration policy effectively implies passworded access.
- `public_ip_address` is optional IPv4. `public_ipv6_address` is optional IPv6. When set, outbound PC92 path data replaces private, loopback, link-local, or otherwise non-public IP literals with the same-family configured public address before advertising them to peers. Existing configs that stored an IPv6 address in `public_ip_address` are accepted and migrated in memory.

### `[telnet]`

Telnet listener settings.

Important fields:

- `host`
- `port` - commonly `587` for Submission/STARTTLS, `465` for implicit TLS/SMTPS, or `25` for a plain local relay
- `ports`
- `feeds`
- `max_clients`
- `idle_timeout_seconds`
- `keepalive_interval_seconds`
- `max_line_length`

`idle_timeout_seconds` controls authentication/input timeout behavior. `keepalive_interval_seconds` independently controls post-login application keepalives; it defaults to 300 seconds and emits a visible prompt while continuing to wait for input. Telnet sockets also enable operating-system TCP keepalive.

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
- `deploy/setup-nginx.sh --sysop-host <host>` is the supported way to publish it through nginx on a dedicated hostname

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

Node-wide MFA policy. Authenticator-app/TOTP challenges use per-user secrets. Email OTP challenges use the `[smtp]` settings and require a valid user email address. Telnet users can enroll TOTP with `set/totp`; after three failed authenticator-code attempts pyCluster removes the TOTP secret and falls back to email OTP until the user enrolls a new secret.

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
- RBN feed spots are stored as normal spots with `source_node` set from this section.
- telnet users do not receive live RBN spots by default; users opt in with `set/rbn` and opt out with `unset/rbn`.
- cluster-peer records and the SysOp access matrix still control whether a peer account may ingest or relay RBN traffic.
- `show/rbn` displays summarized RBN history, while `show/dx` remains the traditional human-posted DX history.
- `accept/rbn` and `reject/rbn` are first-class RBN filter-family commands. `accept/rbn 1 call N9JR` allows only matching RBN/Skimmer spots, while ordinary spot filters remain in the `spots` family.
- use `config/pycluster.local.toml` for host-specific feed credentials

## Example Paths

Default deployed layout:

- config: `/home/pycluster/pyCluster/config/pycluster.toml`
- local override: `/home/pycluster/pyCluster/config/pycluster.local.toml`
- database: `/home/pycluster/pyCluster/data/pycluster.db`
- CTY data: `/home/pycluster/pyCluster/data/cty.dat`
- wpxloc.raw data: `/home/pycluster/pyCluster/data/wpxloc.raw` when you use the standard refresh path
- satellite keps/TLE data: `/home/pycluster/pyCluster/data/keps.txt`

## Operational Advice

- keep web listeners local and publish them through a reverse proxy
- use `config/pycluster.local.toml` for hostnames, SMTP credentials, QRZ credentials, and any other host-local settings you do not want overwritten during repo updates
- use realistic `max_clients` values for your hardware
- back up the base config, local override, and SQLite DB together
- do not hand-edit the live CTY file unless you need an emergency local override
- keep SMTP credentials in `config/pycluster.local.toml`, not the tracked base config
- keep local satellite keps data under `data/` and point `[satellite].keps_path` at that file
- the installed `pycluster-data-refresh.timer` validates and refreshes CTY, WPXLOC, and Keps every six hours; `deploy/doctor.sh` reports the configured Keps path and file age
- CTY data is used for enrichment and review cues such as suspicious spot-prefix flags in the sysop web UI; it is not treated as a complete worldwide legal callsign authority
- ordinary user access should be managed through the registration and verified-email policy, not by relying only on the older `require_password` toggle


## Dataset Status

- The System Operator Console and telnet `show/configuration` report `CTY.DAT` and `wpxloc.raw` status, path, and version/date when detectable.
- The left navigation in the System Operator Console also shows compact pills for the currently loaded country datasets.
- Suspicious spot-prefix review uses CTY and `wpxloc.raw` as operational cues. If country data is missing or stale, pyCluster reports an advisory state instead of hard-flagging calls as suspicious solely because the prefix could not be recognized.
