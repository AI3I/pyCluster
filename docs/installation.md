# Installation

## Requirements

- Linux
- Python 3.11+
- `systemd` for the supported deployment path

Recommended:

- reverse proxy for public exposure
- fast local storage for SQLite
- `fail2ban`

## Validated Platforms

The deploy scripts have been validated on:

- Debian 12 and 13
- Ubuntu 24.04 LTS and 25.10
- Fedora 42 and 43 with SELinux enforcing
- CentOS Stream 9 and 10 with SELinux enforcing
- AlmaLinux 8, 9, and 10 with SELinux enforcing
- Rocky Linux 8, 9, and 10 with SELinux enforcing

Not yet directly validated:

- Raspberry Pi OS / Raspbian
- Red Hat Enterprise Linux
- Oracle Linux

Support guidance:

- RHEL-family support is strongly indicated by the validated CentOS Stream, AlmaLinux, and Rocky Linux paths
- Red Hat Enterprise Linux should be described as expected to work on 9/10-class systems, but not yet directly tested
- Oracle Linux is likely to work as an EL-family target, but it should stay in the unvalidated bucket until it is tested directly
- Raspberry Pi OS / Raspbian is plausible on 64-bit Debian-family images, but should not be claimed as tested yet

Do not target older distro baselines for the supported deployment path:

- Debian 11
- Ubuntu 22.04 LTS
- CentOS 7
- Red Hat Enterprise Linux 7 and below
- Oracle Linux 7 and below

Reason:

- pyCluster requires Python 3.11+
- older distro baselines are too old for the current dependency/runtime requirements

## Hardware and Resource Guidance

Minimum practical node:

- 1 vCPU
- 1 GB RAM
- 10 GB disk

Recommended small production node:

- 2 vCPU
- 2 GB RAM
- 20 GB SSD-backed disk

Additional notes:

- 1 GB RAM works, but leaves less headroom during package operations and service restarts
- EL-family hosts with 1 GB RAM may require temporary swap during package installation; the deploy scripts handle that automatically
- if you plan to run reverse proxy, TLS, `fail2ban`, and longer spot retention on the same host, prefer 2 GB RAM or better

## Local Development Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Run the core service:

```bash
pycluster --config ./config/pycluster.toml serve
```

## Production Install

From the repo root:

```bash
sudo ./deploy/install.sh
sudo ./deploy/doctor.sh
```

This installs:

- application tree under `/home/pycluster/pyCluster`
- `pycluster.service`
- `pyclusterweb.service`
- `pycluster-cty-refresh.timer`
- `fail2ban` filters and jails for pyCluster auth failures
- an initial `SYSOP` account bootstrap note at `/root/pycluster-initial-sysop.txt`

## Upgrade

```bash
sudo ./deploy/upgrade.sh
sudo ./deploy/doctor.sh
```

## Repair

```bash
sudo ./deploy/repair.sh
```

## Uninstall

Keep config and data:

```bash
sudo ./deploy/uninstall.sh
```

Remove config and data too:

```bash
sudo KEEP_CONFIG=0 KEEP_DATA=0 ./deploy/uninstall.sh
```

## First Checks

```bash
systemctl status pycluster.service pyclusterweb.service
sudo ./deploy/doctor.sh
```

If the first install created the bootstrap account successfully, you should also see:

```bash
sudo ls -l /root/pycluster-initial-sysop.txt
```

That file contains the one-time generated `SYSOP` password for first web-based operator login.

## Default Ports

- telnet: `7300`
- sysop web: `8080`
- public web: `8081`

## Optional Dependencies

Serial/KISS support:

```bash
pip install pyserial
```
