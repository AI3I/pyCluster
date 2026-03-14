# Changelog

## Unreleased

### Added

- System Operator web console with user, peer, runtime, protocol, audit, and security views
- public web user login, posting, watch rules, and profile editing
- shipped `fail2ban` filters and jails for auth failures
- structured auth-failure logging
- weekly CTY refresh service and timer

### Changed

- major telnet output cleanup for readability and 80-column friendliness
- explicit `sysop/*` telnet namespace
- improved cluster and link visibility in telnet and web
- improved peer management and access controls

### Fixed

- graceful service shutdown for active telnet sessions
- duplicate live spot rendering on multi-link ingest
- CTY gaps such as `TX5EU`
- stale and confusing System Operator UI behaviors
