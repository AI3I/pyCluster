# Changelog

All notable changes to pyCluster should be recorded here.

## Unreleased

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
