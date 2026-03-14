# DXCluster Recon (AI3I node)
Date: 2026-03-01 UTC
Target: root@dxcluster.ai3i.net (/home/dxcluster/spider)

## Runtime confirmed
- Process: `/usr/bin/perl -w /spider/perl/cluster.pl`
- Listeners:
  - `0.0.0.0:7300`
  - `0.0.0.0:7373`
  - `0.0.0.0:8000`
  - internal `127.0.0.1:27754`
- Version file: `/home/dxcluster/spider/perl/Version.pm`
  - `1.55`, build `166`, git `4868adf[i]`
- Runtime banner shows: `V1.55 build 0.203 (git: 448838ed[r])`

## On-disk data artifacts (live)
- Spots: `/home/dxcluster/spider/data/spots/YYYY/DDD.dat`
- Daily stats: `/home/dxcluster/spider/data/spots/YYYY/DDD.dys`
- Cluster log: `/home/dxcluster/spider/data/log/YYYY/MM.dat`
- Debug/protocol trace: `/home/dxcluster/spider/data/debug/YYYY/DDD.dat`
- WCY: `/home/dxcluster/spider/data/wcy/YYYY/MM.dat`
- WWV: `/home/dxcluster/spider/data/wwv/YYYY/MM.dat`

## Sample formats
### spots/2026/060.dat
- Caret-separated records.
- Example:
  - `7109.9^K3AJ^1772335320^RTTY^WW5L^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42`
- Inferred core fields:
  - freq, dx_call, epoch, info, spotter, dxcc-ish numeric(s), source node, hop/ttl-ish fields, optional IP.

### spots/2026/059.dys
- Caret-separated daily aggregate table.
- First row starts with `TOTALS^...`
- Subsequent rows appear per call with per-band/per-bucket counters.

### log/2026/03.dat
- Caret-separated event log.
- Example:
  - `1772323200^DXProt^PC92A IK6ZDF -> 95.236.67.230 on GB7BAA`
  - `1772323200^ann^ALL^IZ5ILU-6^Dxspider telnet ...`
- Contains protocol events (PCxx), announce lines, start/stop lines.

### debug/2026/060.dat
- High-value protocol trace, includes raw-ish PC frames.
- Example:
  - `1772323200^<- I WB3FFV-2 PC61^1928.0^Z66BCC^ 1-Mar-2026^0000Z^ ^DL6NBC^DA0BCC-7^84.163.40.20^H28^~`
  - `1772323200^<- I WB3FFV-2 PC92^EA8URL-2^0.01^K^...^mojo/c3350180[r]^H91^`
- This is the best source for inter-node compatibility fixtures.

### wcy/2026/03.dat
- Example:
  - `1772335080^139^11^3^0^53^qui^qui^no^DK0WCY^DA0BCC-7`

### wwv/2026/03.dat
- Example:
  - `AE5E^1772335080^141^8^3^No Storms -> No Storms^AE5E^0`

## Live telnet behavior (captured)
Session to `127.0.0.1:7300` with callsign `N0CALL`:
- Prompts for `login:`
- Returns greeting + MOTD + cluster stats + prompt
- Prompt format:
  - `N0CALL de AI3I-15  1-Mar-2026 0327Z dxspider >`
- `show/version` output:
  - `DX Spider Cluster version 1.55 (build 0.203 git: 448838ed[r]) on Linux`
- `show/dx 3` output emits 3 spots in classic formatted rows.

## Why this is enough to build replacement
- Command surface and parsers can be cloned from `/home/dxcluster/spider/cmd/*`.
- Wire compatibility can be built/tested from:
  - `data/debug/...` PC frame traces
  - `data/log/...` event history
  - live telnet transcript behavior
- Interop targets are explicit in code (`set/arcluster`, `set/dxnet`, `set/clx`) and live link config (`connect/wb3ffv-2`).
