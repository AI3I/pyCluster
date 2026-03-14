# DXCluster Compatibility Matrix

Generated UTC: 2026-03-01T03:33:12.798019+00:00

## Baseline

- Target node: `root@dxcluster.ai3i.net`
- Baseline software: DXSpider 1.55 family (runtime reports build 0.203)
- Fixture source: live node data archives + live telnet session

## Data Coverage

- `spots`: 2417
- `spots_daily_rows`: 2853
- `log_events`: 8942
- `debug_protocol_frames`: 15977
- `debug_other_lines`: 687
- `wcy_records`: 4
- `wwv_records`: 2
- `command_files`: 284

## Telnet Behavior (Observed)

- Login prompt seen: `True`
- Welcome banner seen: `True`
- Commands observed in capture: `show/version, show/dx 3`

## Command Surface (By Group)

| Group | Count | Priority |
|---|---:|---|
| `show` | 66 | P0 |
| `set` | 66 | P0 |
| `unset` | 42 | P0 |
| `load` | 14 | P1 |
| `stat` | 9 | P1 |
| `clear` | 6 | P1 |
| `reject` | 5 | P1 |
| `accept` | 5 | P1 |
| `forward` | 2 | P1 |
| `delete` | 2 | P1 |
| `wx.pl` | 1 | P1 |
| `wwv.pl` | 1 | P1 |

## Protocol Frames (Observed in Debug Logs)

| PC Type | Count | Notes |
|---|---:|---|
| `PC92` | 12926 | Implement early |
| `PC61` | 1831 | Implement early |
| `PC11` | 698 | Implement after core |
| `PC24` | 327 | Implement after core |
| `PC51` | 82 | Implement after core |
| `PC93` | 79 | Implement early |
| `PC50` | 24 | Implement after core |
| `PC73` | 4 | Implement after core |
| `PC23` | 2 | Implement after core |
| `PC17` | 1 | Implement after core |
| `PC18` | 1 | Implement after core |
| `PC22` | 1 | Implement after core |

## Minimum Compatible MVP

1. Telnet login/banner/prompt matching the baseline transcript.
2. Spot ingest and persistence compatible with caret-separated spot records.
3. `show/version` and `show/dx` output shape compatible with baseline.
4. Parser/serializer for `PC61`, `PC92`, `PC93` node traffic.
5. Replay tests using captured `debug/latest_debug.dat` frames.

## Reference Files

- Raw manifest: `/home/jdlewis/dxcluster-compat/fixtures/raw/manifest.env`
- Normalized fixtures: `/home/jdlewis/dxcluster-compat/fixtures/normalized/fixtures.json`
- Summary: `/home/jdlewis/dxcluster-compat/fixtures/normalized/summary.json`
- Command inventory entries: `284`
