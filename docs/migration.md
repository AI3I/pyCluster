# Migration

This page documents the first migration path from DXSpider into pyCluster.

## Current Scope

The first migration pass imports:

- user records from `user_asc` or `user_json`
- display name
- QTH
- grid / QRA
- home node
- MOTD from `local_data/motd`
- bad-word rules from `local_data/badword`
- saved outbound peer definitions from simple DXSpider `connect/*` scripts
- exact IP entries from `local_data/badip.local` exported into pyCluster's fail2ban block list

In practical terms, that means:

- DXSpider users become pyCluster local user records
- user profile values are written into the pyCluster SQLite registry and user preferences
- `local_data/motd` becomes the pyCluster node MOTD
- `local_data/badword` becomes the pyCluster bad-word policy list
- simple DXSpider dial-out node definitions become saved pyCluster dial-out peers
- exact IPs from `badip.local` become a pyCluster-managed fail2ban import list at:
  - `config/fail2ban-badip.local`

CIDR entries from `badip.local` are still reported, but they are not applied to fail2ban automatically.

## Not Migrated Yet

This first pass does not migrate:

- passwords
- web frontend customizations
- CIDR or unsupported `badip.local` entries
- unsupported or ambiguous DXSpider `connect/*` scripts
- custom DXSpider web content such as `dxweb`
- every DXSpider local policy artifact just because it exists

## Source Layout

`deploy/migrate.sh` expects a DXSpider tree or `local_data` path such as:

- `/spider`
- `/home/sysop/spider`
- `/spider/local_data`

## Dry Run

Use dry-run first to confirm the source path, pyCluster config, and target SQLite DB:

```bash
sudo ./deploy/migrate.sh --from-dxspider /spider --dry-run
```

## Import

Typical import on a host where pyCluster is already installed:

```bash
sudo ./deploy/migrate.sh --from-dxspider /spider
```

If your pyCluster config is in a different location:

```bash
sudo ./deploy/migrate.sh --from-dxspider /spider --config /path/to/config/pycluster.toml
```

## Output

The migration helper prints a JSON report showing:

- resolved DXSpider source
- resolved `local_data` directory
- imported user count
- skipped user-record count
- whether MOTD was imported
- how many bad-word rules were imported
- how many saved peer definitions were imported
- which peer names were imported
- whether `badip.local` entries were detected
- which `badip.local` entries were exported to the pyCluster fail2ban list
- which `badip.local` entries were skipped
- warnings for unsupported migration items

The report is intended to answer:

- what source tree was actually used
- what pyCluster DB was targeted
- how much user data was brought across
- whether MOTD and bad-word policy were imported
- whether peer definitions were discovered and converted
- whether legacy IP blocks were exported for fail2ban reconciliation

## Recommended Workflow

1. install pyCluster on the target host
2. confirm the target config and DB path
3. run the migration with `--dry-run`
4. run the real import
5. review the JSON report
6. start pyCluster and verify:
   - users
   - MOTD
   - bad-word rules
   - home-node values
   - saved outbound peers

## Notes

- user import reuses the existing DXSpider user-file parser already present in pyCluster
- the migration writes into the same SQLite store used by the running node
- simple DXSpider peer imports currently support:
  - `connect telnet <host> <port>`
  - `'login: ' '<localcall>'`
  - `client <peercall> telnet`
- unsupported connect scripts are reported, not guessed
- imported exact IPs from `badip.local` are written to `config/fail2ban-badip.local`
- install, upgrade, repair, and migration runs reconcile that list into the active pyCluster fail2ban jails
- migration is intentionally conservative:
  - if a DXSpider artifact does not map cleanly and honestly into pyCluster, it is reported rather than silently translated
