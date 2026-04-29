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

## Peer Roles

### Dial-out

Configured outbound peers.

These have:

- a transport address
- a family
- retry behavior
- optional peer password

### Accepted

Inbound peers that connect to the local node.

These do not require:

- a DSN/transport address on the local side
- local retry logic

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

## Operator Views

Useful visibility commands:

- `show/links`
- `show/node`
- `show/connect`

Useful sysop commands:

- `sysop/connect`
- `sysop/disconnect`

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
