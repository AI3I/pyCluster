# pyCluster Roadmap

Updated: 2026-09-03

This roadmap describes product direction after `1.0.15` during `1.0.16`
development. It is not a release checklist. The active GitHub milestone and
issues are the authority for scheduled work; [CHANGELOG.md](../CHANGELOG.md)
records what has shipped.

## Product Direction

pyCluster remains a standalone, decentralized DX cluster implementation. It
should interoperate with established cluster families where wire compatibility
matters without copying their user interface, identity, or every historical
quirk.

Core principles:

- one database-backed account, preference, access, and filter model across
  telnet and web
- clear separation between transport address, peer family, and authenticated
  node identity
- conservative protocol output and strict validation of incoming frames
- useful operator visibility without exposing credentials, private addresses,
  user data, or internal configuration
- no required central registry and no phone-home behavior
- managed runtime datasets rather than stale copies packaged in source

## Current Foundation

The following are implemented foundations, not future roadmap items:

- shared telnet, public-web, and SysOp account state
- registration, password, e-mail verification, and MFA workflows
- per-surface access controls and a SysOp access matrix
- database-backed user filters, watch lists, buddies, and preferences
- public-web authenticated posting and user profile controls
- RBN ingest, mode controls, status, and per-user filtering
- peer management, protocol health, policy-drop history, and audit views
- IPv4, IPv6, and dual-stack listener configuration
- local or external reverse-proxy deployment
- managed CTY, WPXLOC, and KEPS refresh state
- negotiated pyCluster-only `PY` metadata and topology exchange

## 1.0.15 Delivered Focus

The `1.0.15` release focused on behavior spanning several
interfaces and protocols:

1. Authentication and registration recovery, including consistent web and
   telnet behavior.
2. RBN throughput, filtering, backpressure, and operator-visible feed state.
3. DX protocol interoperability, especially identity, addressing, and route
   payload correctness.
4. Propagation calculations and agreement between telnet and web presentation.
5. Live multi-node validation of the private `PY` protocol.
6. Installation and upgrade behavior on Debian-family and SELinux systems,
   including deployments that use an external reverse proxy.
7. VE7CC/CC11 client compatibility, including Ham Radio Deluxe login,
   historical spot initialization, and live spot delivery.

See the [1.0.15 milestone](https://github.com/AI3I/pyCluster/milestone/12)
for issue-level acceptance details. Issue state can change more quickly than
this document.

## pyCluster Protocol

The private `PY` protocol activates only after a normal authenticated peer link
positively identifies both ends as pyCluster and negotiates compatible
capabilities. Legacy and unknown peers must never receive `PY` frames.

Current protocol families cover capability negotiation, node information,
controlled topology gossip, health, dataset freshness, RBN state, notices,
policy summaries, clock state, requests, and errors. Sharing controls belong in
Node Settings and remain local operator policy.

Near-term protocol work:

- test negotiation, reconnect, expiry, and sequence handling between real nodes
- keep digest-before-detail topology exchange bounded and rate limited
- improve status and rejection diagnostics without logging sensitive payloads
- document compatibility rules when the private protocol version changes
- keep public topology views conservative and explicitly labeled as reported

The protocol must never carry passwords, tokens, private keys, user lists,
private messages, registration queues, logs, internal addresses, or remote
configuration mutations.

## Interoperability

DXSpider and other clusterware remain valuable references for established wire
formats and operator expectations. They are comparison sources, not templates
for pyCluster identity or UX.

Work in this area should:

- compare serializers and parsers against captured protocol evidence
- test both IPv4 and IPv6-facing deployments
- prevent private, loopback, wildcard, or placeholder addresses from leaking
  into advertised protocol data
- preserve pyCluster's own software/version identity
- add regression fixtures for every confirmed interoperability defect

The generated [DXSpider parity matrix](dxspider-parity-matrix.md) checks whether
historical command names resolve to concrete pyCluster paths. It does not prove
identical semantics or output.

## Later Work

Potential later work, after the current account and protocol surfaces are
stable:

- an optional SSH console with terminal-aware presentation
- carefully scoped profile federation with explicit trust and conflict rules
- richer propagation and dataset diagnostics
- optional external enrichment that fails closed and never blocks core service
- broader automated multi-node and mixed-cluster compatibility testing

Federated identity is intentionally deferred. It requires a separate security
and data-ownership design and must not be inferred from topology sharing.

## Non-Goals

- cloning DXSpider or another cluster product
- requiring a central pyCluster directory
- broadcasting full topology snapshots on every change
- relaying a full RBN stream through the metadata protocol
- allowing one node to mutate another node's users or configuration
- preserving inert legacy commands solely to inflate a parity count

## Acceptance Standard

A roadmap item is complete only when its database behavior, telnet behavior,
web behavior, protocol behavior, tests, operator documentation, migration path,
and security implications agree where applicable.
