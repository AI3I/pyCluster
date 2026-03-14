# Configuration

Primary config file:

- `config/pycluster.toml`

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

### `[public_web]`

Public web UI and API.

Important fields:

- `enabled`
- `host`
- `port`
- `static_dir`
- `cty_dat_path`

### `[store]`

Persistent SQLite storage.

Important field:

- `sqlite_path`

## Example Paths

Default deployed layout:

- config: `/home/pycluster/pyCluster/config/pycluster.toml`
- database: `/home/pycluster/pyCluster/data/pycluster.db`
- CTY data: `/home/pycluster/pyCluster/fixtures/live/dxspider/cty.dat`

## Operational Advice

- keep web listeners local and publish them through a reverse proxy
- use realistic `max_clients` values for your hardware
- back up the config file and SQLite DB together
- do not hand-edit the live CTY file unless you need an emergency local override
