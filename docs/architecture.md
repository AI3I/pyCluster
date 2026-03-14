# Architecture

pyCluster is a Linux-first DX cluster core with three main interface surfaces and a shared persistence layer.

## Main Components

### Core Service

Implemented in the main application process.

Responsibilities:

- telnet server
- command engine
- user/session model
- node-link handling
- protocol enforcement
- storage access
- System Operator web console

### Public Web Service

Separate process for the user-facing web UI.

Responsibilities:

- public spot/history/network views
- user login
- user posting
- watch lists and profile editing

### Storage

SQLite is the default persistent store.

Used for:

- spots
- messages
- user prefs
- registry records
- protocol/operator state

## Interface Model

### Telnet

The conservative, compatibility-friendly interface.

Design goals:

- readable output
- good command discovery
- legacy-friendly behavior where it matters

### System Operator Web

The browser control plane for local operations.

Design goals:

- visibility
- ease of management
- lower operational friction

### Public Web

The browser UI for ordinary users.

Design goals:

- usability
- policy-aware posting
- good visibility without telnet

## Security Model

Security is layered:

- callsign/password auth
- local access matrix
- callsign blocking
- auth-failure logging
- fail2ban integration

## Data Quality Model

CTY data is local and refreshable.

Design goals:

- stable bundled baseline
- optional refresh from Country Files
- local overrides when urgent real-world exact calls appear before operators refresh

## Node-Link Model

pyCluster separates:

- transport address
- cluster family

This allows the system to distinguish:

- how to connect
- how to behave after the link is up

## Design Philosophy

pyCluster is not trying to be a byte-for-byte reimplementation of any single legacy codebase.

It is trying to:

- preserve interoperability
- preserve useful operator expectations
- improve visibility, deployment, and usability
- keep one coherent model across telnet, web, and operator tooling
