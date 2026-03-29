# Feature Highlights

This page summarizes the features that make pyCluster more than a protocol-compatibility exercise.

## Operator Experience

### Cleaner Telnet UX

pyCluster keeps the familiar telnet interface, but improves:

- prompt clarity
- command discovery
- human-readable replies
- 80-column-friendly formatting
- link and protocol visibility

Examples of improvements:

- `N0CALL-1> ` for normal users
- `N0CALL-1# ` for sysops
- `Password updated for N0CALL.` instead of raw `password=...`
- `show/links` and `show/node` focused on operator usefulness

### Explicit System Operator Command Model

Legacy systems often blur normal-user and operator actions together. pyCluster does not.

pyCluster uses:

- normal user commands for self-service
- `sysop/*` for privileged operator actions

That keeps the mental model cleaner and avoids overloading normal user verbs.

### Less Defensive Tuning

pyCluster tries to solve more network-protection problems in the core instead of expecting the sysop to hand-maintain elaborate defensive filters just to keep the node healthy.

That includes:

- built-in duplicate and loop-resistant behavior
- protocol-health visibility and policy-drop accounting
- peer cleanup and safer default link behavior
- optional filters and policy controls for unusual cases, without making them mandatory for normal operation

## Web Interfaces

### Public Web UI

The public web UI gives normal users a first-class browser experience.

Highlights:

- live spots with filters
- rare-entity badge support
- cluster overview
- watch lists and profiles
- operate tab for login and posting
- in-browser profile editing

### System Operator Web Console

The sysop UI is a real operational console, not just a stats page.

Highlights:

- user management
- access matrix editing
- blocked-user handling
- peer/link management
- protocol thresholds and history
- audit and security views
- runtime and at-a-glance node state

## Security and Abuse Resistance

pyCluster includes several security improvements that are often weak or ad hoc on legacy systems.

Highlights:

- callsign blocking with reason tracking
- per-user channel and posting controls
- structured auth-failure logging
- shipped fail2ban support
- current-ban and recent-failure visibility in the sysop UI

## Operational Improvements

### Deployment

pyCluster ships with a deployment path instead of expecting operators to invent one.

Highlights:

- `systemd` services
- install, upgrade, repair, and uninstall scripts
- doctor script for host checks
- validated deployment on Debian 12/13, Ubuntu 24.04/25.10, Fedora 42/43, and SELinux-enforcing EL-family hosts
- Python 3.11+ baseline, with older distro generations intentionally left out of the supported path

### CTY Data Management

Instead of relying on whatever CTY file happens to exist on a host:

- pyCluster ships a bundled `cty.dat`
- install/upgrade attempt a best-effort refresh
- weekly refresh is supported by timer

### Better Observability

pyCluster tries to make node operation visible instead of opaque.

Highlights:

- audit view
- security view
- protocol summary/history
- policy-drop visibility
- link health and last-frame visibility

## Compatibility Without Imitating Every Quirk

pyCluster aims to interoperate with legacy cluster software where needed, especially DXSpider-family behavior, without preserving every awkward UI behavior just because it is old.

The design goal is:

- strict where wire behavior matters
- better where user and operator experience can clearly improve
