# Node Linking

pyCluster supports direct node links and keeps compatibility-focused behavior for legacy cluster families.

## Concepts

### Transport Address

How pyCluster opens the connection.

Examples:

- `tcp://host:port`
- `dxspider://host:port?login=AI3I-16&client=N9JR-2`
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

## DXSpider DSN Example

```text
dxspider://dx.n9jr.com:7300?login=AI3I-16&client=N9JR-2
```

If the remote peer requires a password:

```text
dxspider://dx.n9jr.com:7300?login=AI3I-16&client=N9JR-2&password=secret
```

## Operator Views

Useful visibility commands:

- `show/links`
- `show/node`
- `show/connect`

Useful sysop commands:

- `sysop/connect`
- `sysop/disconnect`

## Notes

- pyCluster avoids silently guessing peer family/version
- explicit identity learned from protocol traffic is preferred over inference
- loop suppression is surfaced in operator views and policy-drop summaries
