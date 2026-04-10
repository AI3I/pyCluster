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
- `require_password`
- `registration_required`
- `verified_email_required_for_web`
- `verified_email_required_for_telnet`
- `initial_grace_logins`

Auth policy notes:

- `registration_required` is now the primary ordinary-user gate.
- When `registration_required = true`, ordinary human users must have a local record before account activation.
- When `verified_email_required_for_web = true`, ordinary web login requires a verified email address.
- When `verified_email_required_for_telnet = true`, ordinary telnet login requires a verified email address and unverified users are driven through telnet email verification.
- `initial_grace_logins` controls how many failed or skipped telnet verification attempts are allowed before the pending account is locked.
- `require_password` is now a narrower legacy telnet-password toggle. For ordinary human users, the stronger registration policy effectively implies passworded access.

### `[telnet]`

Telnet listener settings.

Important fields:

- `host`
- `port`
- `max_clients`
- `idle_timeout_seconds`
- `max_line_length`

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

### `[store]`

Persistent SQLite storage.

Important field:

- `sqlite_path`

### `[smtp]`

Node-wide email delivery settings used for SMTP-backed MFA.

Important fields:

- `host`
- `port`
- `username`
- `password`
- `from_addr`
- `from_name`
- `starttls`
- `use_ssl`
- `timeout_seconds`

### `[mfa]`

Node-wide email OTP policy.

Important fields:

- `enabled`
- `require_for_sysop`
- `require_for_users`
- `issuer`
- `otp_ttl_seconds`
- `otp_length`
- `max_attempts`
- `resend_cooldown_seconds`

## Example Paths

Default deployed layout:

- config: `/home/pycluster/pyCluster/config/pycluster.toml`
- local override: `/home/pycluster/pyCluster/config/pycluster.local.toml`
- database: `/home/pycluster/pyCluster/data/pycluster.db`
- CTY data: `/home/pycluster/pyCluster/fixtures/live/dxspider/cty.dat`
- wpxloc.raw data: `/home/pycluster/pyCluster/fixtures/live/dxspider/wpxloc.raw` when you use the standard refresh path

## Operational Advice

- keep web listeners local and publish them through a reverse proxy
- use `config/pycluster.local.toml` for hostnames, SMTP credentials, QRZ credentials, and any other host-local settings you do not want overwritten during repo updates
- use realistic `max_clients` values for your hardware
- back up the base config, local override, and SQLite DB together
- do not hand-edit the live CTY file unless you need an emergency local override
- keep SMTP credentials in `config/pycluster.local.toml`, not the tracked base config
- CTY data is used for enrichment and review cues such as suspicious spot-prefix flags in the sysop web UI; it is not treated as a complete worldwide legal callsign authority
- ordinary user access should be managed through the registration and verified-email policy, not by relying only on the older `require_password` toggle


## Dataset Status

- The System Operator Console and telnet `show/configuration` report `CTY.DAT` and `wpxloc.raw` status, path, and version/date when detectable.
- The left navigation in the System Operator Console also shows compact pills for the currently loaded country datasets.
- Suspicious spot-prefix review uses CTY and `wpxloc.raw` as operational cues. If country data is missing or stale, pyCluster reports an advisory state instead of hard-flagging calls as suspicious solely because the prefix could not be recognized.
