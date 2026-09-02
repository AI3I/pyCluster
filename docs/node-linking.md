# Node Linking

pyCluster supports direct node links and keeps compatibility-focused behavior for legacy cluster families.

## Concepts

### Transport Address

How pyCluster opens the connection.

Examples:

- `tcp://host:port`
- `pycluster://host:port?login=LOCALNODE-1&client=PEERNODE-1`
- `dxspider://host:port?login=LOCALNODE-1&client=PEERNODE-1`
- `kiss:///dev/ttyUSB0?baud=9600`
- `ax25://DESTCALL?source=MYCALL&via=DIGI1,DIGI2`

### Cluster Family

How pyCluster behaves after the connection is established.

Supported labels:

- `dxspider`
- `arcluster`
- `dxnet`
- `clx`
- `pycluster`

### Peer Account

Node linking is not only a transport setting. A peer also needs an account on the node it logs into.

For inbound links to this pyCluster node:

- create or update a local account for the remote node callsign, including SSID when used
- set that account's `node_family` to the expected cluster family, such as `pycluster` or `dxspider`
- set a password when the remote side is expected to authenticate with one
- make sure the account is not blocked and is allowed to log in through telnet/node-link access

For outbound links from this pyCluster node:

- the remote sysop must create the corresponding account for this node's login callsign
- the `login=` value in the DSN must match what the remote node expects
- the optional DSN `password=` value, or saved peer password, must match the remote account when the remote node requires authentication

A saved peer definition tells pyCluster how to connect. The peer account tells the receiving node who is allowed to connect and what node-family behavior to apply after login.

## Peer Roles

### Dial-out

Configured outbound peers.

These have:

- a transport address
- a family
- retry behavior
- optional peer password
- a matching account on the remote node for the local login callsign

### Accepted

Inbound peers that connect to the local node.

These do not require:

- a DSN/transport address on the local side
- local retry logic

They still require:

- a local account for the remote peer callsign
- a configured node family on that account
- any password or access policy required by the local node

## pyCluster DSN Example

```text
pycluster://example.net:7300?login=LOCALNODE-1&client=PEERNODE-1
```

If the remote peer requires a password:

```text
pycluster://example.net:7300?login=LOCALNODE-1&client=PEERNODE-1&password=secret
```

## DXSpider DSN Example

```text
dxspider://example.net:7300?login=LOCALNODE-1&client=PEERNODE-1
```

If the remote peer requires a password:

```text
dxspider://example.net:7300?login=LOCALNODE-1&client=PEERNODE-1&password=secret
```

Peers with the `dxspider`/`spider` cluster family receive legacy PC11 spot relay frames. pyCluster peers continue to receive PC61 relay frames.

When pyCluster sends PC18, its software field identifies `pyCluster <version>` rather than claiming to be DXSpider or borrowing DXSpider's presentation wording. The `5457` protocol field remains for wire-protocol compatibility. For outbound `dxspider://` connections, pyCluster records DXSpider's PC18 banner but does not answer with PC18 because DXSpider deliberately ignores that frame on inbound non-CC-Cluster links. The transport sends one PC20 to complete startup, followed by the legacy initialization frames.

Steady-state DXSpider liveness uses PC51 ping requests and replies. pyCluster does not send periodic PC20 frames to DXSpider peers because DXSpider interprets PC20 as completion of startup configuration and retransmits PC19/PC22 configuration in response. Native pyCluster links continue to use their existing PC20 heartbeat.

## pyCluster Protocol Discovery

pyCluster reserves the `PY` frame family for pyCluster-to-pyCluster extensions. The feature is enabled by default through `[py_protocol].enabled` and is attempted only on an authenticated link whose configured peer family is pyCluster or whose remote PC18 positively identifies pyCluster. A System Operator can disable the protocol or any sharing category, and link policy rejects `PY` traffic on every non-pyCluster peer profile.

For links between capable pyCluster nodes, PY is the preferred protocol for every capability it defines. An explicitly configured `pycluster` peer family is treated as an authenticated operator assertion: the node sends its PC18 identity and immediately initiates PY00 without waiting for the remote PC18. The returned PY00 must still match the authenticated peer callsign and advertise compatible capabilities. Unconfigured inbound and unknown-family peers must positively identify through PC18 before PY is attempted. PC remains the fallback when PY is disabled, unsupported, or fails negotiation, and remains the operational transport for spots, announcements, chat, mail, and routing families that do not yet have a PY equivalent. pyCluster does not suppress those PC families until a negotiated PY replacement provides equivalent behavior and delivery guarantees.

Protocol version 1 begins with a direct-peer hello:

```text
PY00^1^HELLO^NODE-CALL^SOFTWARE-VERSION^CAPABILITY-LIST^EPOCH
```

`PY00` is the only bootstrap frame and is sent at most once per connection in each direction. It advertises the authenticated node callsign, pyCluster software version, capabilities actually implemented by that build, and UTC epoch. The receiving node validates the advertised callsign against the authenticated peer and stores the direct-peer metadata in the database. Both sides calculate the intersection of their advertised capabilities; every other `PY` family is rejected until that reciprocal negotiation completes. `PY00` is never relayed.

`PY99` carries bounded errors for unsupported or malformed post-negotiation frames when both sides advertise `py99-error`. A node does not answer malformed `PY00` or `PY99` with another error, preventing error loops.

`PY01` carries a direct peer's NODEINFO record when both sides negotiate `node-info` and the sending operator enables `share_node_info`. The structured record contains the authenticated node callsign, stable installation UUID, monotonic content sequence, pyCluster version, explicitly configured public web URL, locator/QTH, configured contact, enabled-service names, capabilities, update time, and expiry. It contains no bind address or inferred URL. Lower sequences, changed content at the same sequence, expired records, excessive future timestamps, and callsign mismatches are rejected. A changed installation UUID is retained as a direct identity transition for operator review.

When `share_topology` is enabled and both peers negotiate `topology-digest`, `topology-records`, and `request`, topology reconciliation uses three additional frames:

- `PY02 TOPOLOGY_DIGEST` sends paged identity, sequence, content-digest, and expiry summaries rather than full records. Each exchange has a unique snapshot ID and ordered page numbers; receivers wait for the final page before requesting details and reject missing, reordered, or duplicate pages.
- `PY10 REQUEST` asks only for missing, changed-identity, newer-sequence, or conflicting-digest records.
- `PY03 TOPOLOGY_RECORDS` returns only requested records in batches bounded by both `max_records_per_frame` and `max_frame_bytes`.

Each node persists its own known-node catalog. Direct observations outrank relayed reports while the direct record remains valid, origin sequences cannot move backward, and changed content at the same sequence is rejected. A received relayed record increments its hop count and is dropped above `max_hops`. Records expire at their origin-provided time and are pruned locally. A node does not advertise a learned record back to the peer it came from. Digests are exchanged on connection, after learned changes, and periodically at `refresh_seconds` with per-peer jitter; unchanged full records are never flooded. This provides eventual reported visibility without claiming a central or perfectly authoritative network view.

The remaining implemented version 1 metadata families are direct-peer, read-only summaries:

- `PY04 HEALTH` reports aggregate node/service state and the health of the link carrying the frame. It includes bounded receive/transmit times, quiet/flapping indicators, reconnect state, and an error category, never raw errors or logs.
- `PY05 DATASETS` reports CTY.DAT, wpxloc.raw, and KEPS version/date, modification time, stale state, and availability status. It does not transfer dataset contents or local file paths.
- `PY06 RBN_STATUS` reports whether RBN is enabled, explicitly named modes, feed/connection counts, last spot time, recent one-minute ingest rate, and queue state. It never carries feed credentials, hosts, ports, commands, or spot records.
- `PY07 NOTICE` reports a dedicated operator-controlled normal, maintenance, upgrading, degraded, or testing notice with monotonic sequence, explicit active/cancel state, creation time, and expiry. It is separate from the MOTD and never forwards arbitrary local text implicitly.
- `PY08 POLICY` reports boolean registration, email-verification, MFA, and public/anonymous web availability. It contains no user-specific policy, account, or registration data.
- `PY09 CLOCK` reports UTC epoch, process uptime, and process boot time. The receiver records an observed offset; it does not adjust either node's clock.

Each family has its own bilateral capability and local `share_*` control. These records are sent after negotiation and refreshed no more often than five minutes (or the lower configured `refresh_seconds`). Receivers enforce the authenticated callsign, future-time tolerance, bounded expiry, strict fields and enums, and frame/rate limits. The latest direct-peer records are persisted and exposed under the peer's `proto.py` object in the authenticated SysOp `/api/peers` response.

Inbound and outbound `PY` frames are capped by `max_frame_bytes` and `max_bytes_per_minute`. Negotiation state and rate windows reset on reconnect. Oversized, excessive, unauthenticated, disabled, profile-mismatched, and unnegotiated frames are dropped and counted in peer policy diagnostics.

The `[py_protocol]` controls provide conservative boundaries for implemented and later frame families:

- `share_node_info`, `share_topology`, `share_health`, `share_datasets`, `share_rbn_status`, `share_policy`, `share_clock`, and `share_notices` govern what this node may advertise.
- `max_hops`, `max_records_per_frame`, `refresh_seconds`, and `record_ttl_seconds` constrain topology reconciliation.
- No PY frame contains passwords, tokens, private keys, users, mail, registration records, logs, private addresses, full RBN spot streams, or remote configuration mutations.

Authenticated SysOps can inspect the durable local catalog through `GET /api/py-nodes`. The response labels each record as `local`, `direct`, or `reported` and includes its source, learned-from peer, hop count, first/last seen times, and expiry.

The SysOp Known pyCluster Nodes table also merges direct peers positively
identified by PC18. This lets an operator see that a connected peer is running
pyCluster even when the private protocol is disabled on either end. Such a row
is labeled `identified` and explicitly distinguishes locally disabled PY,
an absent or invalid `PY00` handshake, and a negotiated link that has not
supplied `PY01` NODEINFO. The PC18 software version is descriptive only: a
validated `PY00` proves private-protocol support, and its advertised capability
list is authoritative. Each connection records its own PY00 transmit and valid
receive times, preventing a successful old session from making a new session
look negotiated. Silence after a transmitted PY00 is reported as no valid
response rather than being misclassified as proof that the remote node disabled
or rejected PY. The identified-only row is
not inserted into the durable topology catalog until a validated NODEINFO
record is received.

The SysOp Node Settings > pyCluster Protocol view provides PY sharing controls, field-level NODEINFO privacy, a shared-metadata preview, and the structured network-notice editor. Protocol Health provides the Known pyCluster Nodes catalog and live protocol diagnostics. Sharing-policy changes apply locally immediately; existing links renegotiate newly enabled capabilities after reconnect.

Outbound PC92 path advertisements are sanitized when `node.public_ip_address`, `node.public_ipv6_address`, or a detected global interface address is available. Private, loopback, link-local, `localhost`, and otherwise non-public IPv4/IPv6 literals in outbound PC92 payload fields are replaced with the same-family public address before transmission. Locally generated PC61 spot and PC93 chat/bulletin relays use the same configured-or-detected public address selection for their IP fields.

RBN/skimmer reports are a local live feed and are not forwarded over cluster peer links. This applies both to reports read from a configured RBN connection and reports recognized as RBN after arriving from a peer. Ordinary human-posted cluster spots continue to follow the configured peer relay policy.

Outbound peer passwords are stored separately from transport addresses. The SysOp peer API and editor never return a saved password; they expose only whether one exists. Leaving the password field blank while editing preserves the saved secret, and Connect resolves a missing password from the saved peer record. Entering a new password replaces the saved value.

## Operator Views

Useful visibility commands:

- `show/links`
- `show/node`
- `show/connect`

Useful sysop commands:

- `sysop/peeraccount add <peer-call> <pycluster|dxspider|dxnet|arcluster|clx>`
- `sysop/peeraccount password <peer-call> <password>`
- `sysop/peeraccount show <peer-call>`
- `sysop/peer show [peer]`
- `sysop/peer add <peer> <dsn> [profile]`
- `sysop/peer delete <peer>`
- `connect <peer> <dsn>`
- `disconnect <peer>`

In the System Operator web console, the `Peers and Links` editor now exposes `Cluster Family` as an explicit selector, including `pyCluster` for pyCluster-to-pyCluster links.

## Link Health Labels

Peer role and traffic direction are separate ideas:

- `Dial-out` means this node initiated the socket
- `Accepted` means the remote node connected inbound
- `connected` and `disconnected` describe whether a live socket exists
- `bidirectional`, `receive active`, `transmit active`, `idle`, and `connected quiet` describe recent traffic direction

Protocol-health labels such as stale, degraded, or flapping are based on received PC protocol freshness and configured thresholds. They do not automatically mean the transport socket is down.

## Notes

- pyCluster avoids silently guessing peer family/version
- explicit identity learned from protocol traffic is preferred over inference
- loop suppression is surfaced in operator views and policy-drop summaries
