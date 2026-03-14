# pyCluster Project History

This document summarizes the main Codex implementation thread for `pyCluster`, primarily from session `019ca75a-6e5e-73a2-9bca-10616a2c5f2f` in [~/.codex/history.jsonl](/home/jdlewis/.codex/history.jsonl) and [the matching session log](/home/jdlewis/.codex/sessions/2026/02/28/rollout-2026-02-28T22-04-07-019ca75a-6e5e-73a2-9bca-10616a2c5f2f.jsonl).

## Original motivation

The project started from the observation that legacy DX cluster software, especially DXSpider distribution and source availability, had become difficult to find or verify. The goal became:

- Build a modern replacement in Python rather than depend on disappearing legacy distribution points.
- Treat DXSpider behavior as the primary compatibility target.
- Preserve interoperability with other cluster families where practical.

## Core user constraints

The thread established these requirements early and repeatedly:

- The replacement should be called `pyCluster`.
- It should run on any Linux system.
- Raspberry Pi should be considered, but not drive the architecture.
- The existing live node at `dxcluster.ai3i.net` should be used only for read-only inspection and collection.
- No posting, spotting, or other world-visible traffic should be sent to the live production cluster.
- Compatibility should prioritize real behavior, not superficial command-name parity.

## Early research and target selection

The first phase focused on determining whether a serious compatibility effort was possible.

- DXSpider `1.57` was chosen as the aspirational behavior target.
- The live installed node at `dxcluster.ai3i.net` provided a working DXSpider `1.55` baseline for source and runtime inspection.
- Public cluster listings were analyzed to estimate software-family prevalence.
- Resulting target order:
  - DXSpider first
  - AR-Cluster second
  - CC Cluster third
  - DxNet, CLX, and others later as lower-priority adapters

## Live-node reconnaissance

The next phase inspected the running DXSpider instance at `dxcluster.ai3i.net`.

- Verified DXSpider code and local overrides under `/home/dxcluster/spider`.
- Confirmed ports, runtime process, command tree, and interop hooks.
- Collected on-disk artifacts from:
  - `data/spots`
  - `data/log`
  - `data/debug`
  - `data/wcy`
  - `data/wwv`
- Captured a read-only telnet transcript for login, prompt, and `show/dx` behavior.

This reconnaissance became the factual baseline for parser and compatibility work and is reflected in:

- [fixtures/raw/telnet/session.txt](/home/jdlewis/GitHub/pyCluster/fixtures/raw/telnet/session.txt)

## Compatibility harness phase

The first concrete deliverable was a reproducible harness that could collect and normalize real DXSpider data.

- Added remote collection scripts for raw samples.
- Added normalizers to convert raw files and transcripts into structured JSON fixtures.
- Added generated compatibility and replay artifacts to measure implementation progress.

This established the fixture pipeline that still underpins `pyCluster` development:

- [scripts/collect_remote_samples.sh](/home/jdlewis/GitHub/pyCluster/scripts/collect_remote_samples.sh)
- [scripts/build_fixtures.py](/home/jdlewis/GitHub/pyCluster/scripts/build_fixtures.py)
- [fixtures/normalized/fixtures.json](/home/jdlewis/GitHub/pyCluster/fixtures/normalized/fixtures.json)
- [docs/replay-report.json](/home/jdlewis/GitHub/pyCluster/docs/replay-report.json)

## MVP server phase

Once fixture coverage was in place, the project shifted from reconnaissance into implementation.

The initial MVP included:

- Async telnet server
- SQLite-backed persistence
- Basic spot import path
- Built-in web admin surface
- Linux-service-oriented CLI and config

That work became the foundation of the current codebase:

- [src/pycluster/telnet_server.py](/home/jdlewis/GitHub/pyCluster/src/pycluster/telnet_server.py)
- [src/pycluster/store.py](/home/jdlewis/GitHub/pyCluster/src/pycluster/store.py)
- [src/pycluster/web_admin.py](/home/jdlewis/GitHub/pyCluster/src/pycluster/web_admin.py)
- [src/pycluster/cli.py](/home/jdlewis/GitHub/pyCluster/src/pycluster/cli.py)

## SH/DX and protocol milestones

After the MVP, the thread focused on behavior fidelity.

Key milestones:

- Implemented parsed `sh/dx` behavior instead of a simple latest-spots listing.
- Added typed protocol support for captured `PC61`, `PC92`, and `PC93` frames.
- Added replay and round-trip tooling to score compatibility against real captured frames.
- Added a local-only node-link engine for isolated inter-node testing.

Relevant artifacts:

- [src/pycluster/shdx.py](/home/jdlewis/GitHub/pyCluster/src/pycluster/shdx.py)
- [src/pycluster/protocol.py](/home/jdlewis/GitHub/pyCluster/src/pycluster/protocol.py)
- [src/pycluster/replay.py](/home/jdlewis/GitHub/pyCluster/src/pycluster/replay.py)
- [src/pycluster/node_link.py](/home/jdlewis/GitHub/pyCluster/src/pycluster/node_link.py)
- [fixtures/normalized/pc_frames.json](/home/jdlewis/GitHub/pyCluster/fixtures/normalized/pc_frames.json)

## Rename and product direction

During implementation, the project was explicitly renamed from `dxcluster-compat` to `pyCluster`.

The rename also clarified the intended identity:

- Linux-first, not Pi-only
- Modernized implementation, not Perl translation
- Compatibility-focused, but with better hardening and admin ergonomics

That direction is reflected in:

- [README.md](/home/jdlewis/GitHub/pyCluster/README.md)
- [pyproject.toml](/home/jdlewis/GitHub/pyCluster/pyproject.toml)
- [config/pycluster.toml](/home/jdlewis/GitHub/pyCluster/config/pycluster.toml)

## Command-surface and parity work

A major later phase focused on command coverage and command behavior depth.

This included:

- Extracting a full DXSpider command catalog from the live source tree
- Building and regenerating parity matrices
- Converting many shallow compatibility handlers into stateful or privilege-gated real behavior
- Implementing shortcut and abbreviation resolution
- Expanding admin, DB, and control-plane commands

Primary parity references:

- [docs/dxspider-command-catalog.md](/home/jdlewis/GitHub/pyCluster/docs/dxspider-command-catalog.md)
- [docs/dxspider-parity-matrix.md](/home/jdlewis/GitHub/pyCluster/docs/dxspider-parity-matrix.md)
- [scripts/generate_dxspider_parity.py](/home/jdlewis/GitHub/pyCluster/scripts/generate_dxspider_parity.py)

## Safety and operational stance

Two project principles stayed consistent through the thread:

- Read-only interaction with the production DXSpider node unless explicitly isolated for testing later.
- Prefer real behavior in `pyCluster`, but put privilege checks, policy controls, and audit visibility around destructive/admin commands.

That led to later control-policy and audit work in the telnet command surface rather than leaving sensitive commands as permanently fake.

## Net result

The implementation thread did not stop at a concept or scaffold. It progressed through:

1. Market and software-family research
2. Live DXSpider inspection
3. Fixture and compatibility harness creation
4. Python MVP server and storage implementation
5. `sh/dx` and protocol replay work
6. Project rename to `pyCluster`
7. Large command-parity and command-behavior expansion

In short: `pyCluster` began as a response to disappearing DXCluster software, used the AI3I DXSpider node as the empirical authority, and evolved into a Linux-first Python replacement effort built around reproducible fixtures, protocol replay, and incremental DXSpider compatibility.
