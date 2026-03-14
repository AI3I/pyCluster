# Installation

## Requirements

- Linux
- Python `3.11+`
- `systemd` for the supported deployment path

Recommended:

- reverse proxy for public exposure
- fast local storage for SQLite
- `fail2ban`

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

## Default Ports

- telnet: `7300`
- sysop web: `8080`
- public web: `8081`

## Optional Dependencies

Serial/KISS support:

```bash
pip install pyserial
```
