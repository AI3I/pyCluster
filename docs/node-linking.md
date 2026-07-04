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

Outbound PC92 path advertisements are sanitized when `node.public_ip_address` or `node.public_ipv6_address` is configured. Private, loopback, link-local, and otherwise non-public IPv4/IPv6 literals in outbound PC92 payload fields are replaced with the same-family configured public address before transmission.

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
