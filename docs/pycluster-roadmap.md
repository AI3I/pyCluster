# pyCluster Roadmap

This document captures the current design direction for pyCluster so product,
protocol, and UX decisions are not stranded in chat history.

## Product Direction

pyCluster should be:
- protocol-compatible with existing cluster families where interoperability matters
- its own product in identity, operator experience, and web/admin presentation
- friendlier and more legible than legacy cluster software
- strict on wire behavior, loose on imitation of legacy UI quirks

## Current Priorities

1. Finish the core operator model:
   - telnet UX cleanup
   - sysop prompt/visibility
   - humanized responses
2. Define one clean privilege and access-control model:
   - `user` vs `sysop`
   - channel/capability policy
3. Reuse that same model across every interface:
   - telnet
   - public web
   - System Operator web
   - future SSH
4. Improve link/operator visibility:
   - direct links
   - topology
   - peer version/capability
5. Add higher-level pyCluster-only features:
   - richer cluster map interaction
   - federation
   - richer enrichment sources
6. Add the richer SSH console:
   - secure human access
   - TUI-style layout
   - color and richer terminal behavior

## Telnet UX

### Prompt model

- Normal user prompt:
  - `AI3I-16> `
- System Operator prompt:
  - `AI3I-16# `

This is the primary visual indicator of a privileged session.

### Output style

- Favor 80-column-friendly output by default
- Do not hard-wrap blindly if it destroys useful layout
- Allow opt-out via:
  - `set/nowrap`
  - `unset/nowrap`

### User-facing responses

Avoid variable-dump responses such as:
- `password=`
- `qth=`

Prefer human-readable responses such as:
- `Password updated for AI3I.`
- `Password cleared for AI3I.`
- `QTH updated for AI3I.`
- `Home node for AI3I set to AI3I-16.`

### Help and command discovery

- `help` should be privilege-aware
- `show/commands` should be privilege-aware
- ordinary users should not be flooded with sysop-only commands
- `show/apropos <term>` should remain useful for discovery

## System Operator Command Model

### Principles

- Keep normal user commands stable
- Do not overload self-service commands with ambiguous sysop behavior
- Introduce an explicit `sysop/` namespace for privileged operator actions

### User/self-service commands

Examples:
- first telnet login with no password should force password creation
- `set/password <newpass>`
- `unset/password`
- future SSH onboarding:
  - `set/sshkey <publickey>`
  - `unset/sshkey`
- `set/qra <value>`
- `set/qth <value>`
- `set/name <value>`
- `set/email <value>`
- `set/homenode <node>`

### System Operator commands

Planned examples:
- `sysop/password <call> <newpass>`
- `sysop/clearpassword <call>`
- `sysop/user <call> <field> <value>`
- `sysop/deleteuser <call>`
- `sysop/privilege <call> <user|sysop>`
- `sysop/homenode <call> <node>`
- `sysop/blocklogin <call> <on|off>`

Additional high-value sysop commands:
- `sysop/showuser <call>`
- `sysop/users`
- `sysop/sysops`
- `sysop/sessions`
- `sysop/links`
- `sysop/connect <peer> <dsn>`
- `sysop/disconnect <peer>`
- `sysop/proto`
- `sysop/protohistory`
- `sysop/protoalerts`
- `sysop/policydrop`

## Access-Control Matrix

Access policy should be a first-class feature, not a side effect of scattered prefs.

## RBN Support

pyCluster should support RBN as a first-class feature rather than treating it as generic spot text.

Planned work:
- explicit user controls such as `accept/rbn`, `reject/rbn`, and `set/rbn`
- richer RBN-aware filtering and presentation
- capability awareness when peer software advertises `rbn`
- web and telnet visibility that makes RBN traffic understandable instead of opaque

### Per-user policy dimensions

Login channels:
- telnet
- web
- ssh

Posting capabilities:
- spots
- chat
- announce
- wx
- wcy
- wwv

### CLI design

Readable matrix view:
- `sysop/access <call>`

Example:

```text
AI3I
          telnet  web  ssh
login       on    on   off
spots       on    on   off
chat        on    on   off
announce    on    on   off
wx          on    on   off
```

Control commands:
- `sysop/setaccess <call> <channel> <capability> <on|off>`

## Link Visibility

Legacy cluster software is weak at showing relationship and health clearly.
pyCluster should do better.

### Public cluster map

The public cluster map should stay, but become more useful before SSH work:
- clicking a bubble should filter or highlight matching nodes in the table
- clicking a node row should highlight direct neighbors/links on the map
- direct peers, home node, and pyCluster nodes should be visually distinct
- add a simple legend for:
  - pyCluster
  - direct peer
  - other node
  - saved/down peer

### Views

- `show/cluster`
  - one-line summary
- `show/links`
  - direct peer links and health
- `show/node`
  - local/inferred topology tree where known
- `show/routes`
  - more detailed routing-oriented view if needed

### Useful fields

For direct links:
- peer callsign
- family/profile
- up/down state
- direction
- last activity age
- rx/tx counters
- last frame type
- observed version/capability string

### Example

```text
Peer        Family    State   Dir   Last    RX/TX   LastPC   Version
AI3I-15     spider    up      out   0m      812/94  PC11     1.57 b633
WB3FFV-2    spider    via15   n/a   n/a     n/a     n/a      1.57 b633
```

## Peer Version and Capability Detection

Where protocol banners allow it, pyCluster should record:
- cluster family
- version/build
- capability flags

Examples:
- DXSpider `PC18`
- capability markers such as `pc9x`, `ve7cc`, `rbn`

This information should appear in:
- `show/links`
- `show/node`
- System Operator peer views

## Federation Model

pyCluster should eventually support a federated identity/profile model.

### Core rules

- `home node` is the source of truth for a user
- only the home node may originate authoritative password/profile updates
- foreign nodes cache trusted copies

### Data ownership

The home node should own:
- password hash
- display/profile fields
- registration timestamp
- access policy
- optional privilege assertions

### Conflict rules

- `registered_at`
  - oldest wins
- `updated_at`
  - newest wins, but only when the update comes from the home node

### Expiry

- cached ownership/profile assertions should expire
- initial working default:
  - 30 days

### Transport

This should be a pyCluster-native federation protocol, separate from legacy
DXSpider-style cluster interoperability frames.

Use cases:
- password changes propagating from home node
- profile updates propagating from home node
- trusted-node access and source-of-truth discovery

## SSH Console

Planned future direction:
- add an SSH cluster console
- same command engine as telnet
- no shell access
- callsign/password first
- public key support later

Likely ports:
- `7322`
- optionally `2222`

### Telnet vs SSH presentation

Telnet should remain conservative:
- 80-column-friendly by default
- plain text first
- minimal control-sequence assumptions
- compatible with older clients and basic terminals

SSH can be the enhanced human interface:
- optional ANSI color
- richer ASCII/box-drawing where terminal-safe
- more flexible width handling
- a cleaner modern operator experience than telnet

In other words:
- telnet = compatibility-first terminal
- SSH = preferred modern human console

## Ongoing Interoperability Work

Continue validating against live cluster families:
- DXSpider
- AR-Cluster
- CC-Cluster
- CLX
- DxNet

Shared conventions already observed:
- live spots should use `DX de ...`
- `sh/dx` should be a table/list view
- `wcy` and `wwv` should be tabular when supported

Software-specific areas that do not need one universal presentation:
- prompt style
- version banner
- `show/node`
- `show/cluster`
- `show/users`

## Implementation Sequence

The implementation order below is optimized to minimize rewrite. Each stage
creates the policy or shared behavior needed by the next one.

### Stage 1: Stabilize the Core Runtime

Before adding more surface area:
- keep `AI3I-16` reliably online
- keep reconnect behavior reliable
- avoid half-alive core-service states
- continue live traffic parity checks with `AI3I-15`

This stage should not introduce large new features. It should keep the current
node dependable enough to build on.

### Stage 2: Finish the Telnet Foundation

Complete the base telnet experience first:
- sysop `#` prompt
- privilege-aware `help`
- privilege-aware `show/commands`
- `set/nowrap` / `unset/nowrap`
- human-readable success/failure responses
- 80-column-friendly defaults where practical

Why now:
- this establishes the operator model
- later web and SSH behavior should mirror these semantics

### Stage 3: Add the `sysop/` Namespace

Create a clear privileged command family without breaking user commands:
- `sysop/password`
- `sysop/clearpassword`
- `sysop/user`
- `sysop/deleteuser`
- `sysop/privilege`
- `sysop/homenode`
- `sysop/blocklogin`
- `sysop/showuser`
- `sysop/users`
- `sysop/sysops`
- `sysop/sessions`

Why after Stage 2:
- the prompt and privilege model are already in place
- avoids reworking the telnet help/visibility rules twice

### Stage 4: Implement the Access Matrix

Add the real policy model for all interfaces:
- login by channel:
  - telnet
  - web
  - ssh
- posting by channel/capability:
  - spots
  - chat
  - announce
  - wx
  - wcy
  - wwv

Add:
- `sysop/access <call>`
- `sysop/setaccess <call> <channel> <capability> <on|off>`

Why here:
- this is the shared policy layer
- public web auth/posting and future SSH should use this instead of inventing
  separate rules

### Stage 5: Bring Public Web User Auth and Posting Up to Par

Once the access matrix exists:
- add user login to the public web UI
- allow posting:
  - spots
  - chat
  - announce
  - wx/wcy/wwv where enabled
- enforce per-user channel policy from Stage 4

Why after Stage 4:
- avoids building web auth/posting on temporary rules that must be rewritten

### Stage 6: Improve Link and Network Visibility

Add operator-grade link visibility:
- `show/links`
- better `show/node`
- optional `show/routes`
- peer family/version/capability display
- clearer health and traffic summaries

Why here:
- by this point the user and sysop models are stable
- link display can reuse settled privilege/help conventions

### Stage 7: Add Audit and Operator History

Add a clearer change trail for sensitive operations:
- password changes
- privilege changes
- login blocking
- access-matrix changes
- peer/policy changes

Why here:
- once sysop commands and access controls exist, operator auditability becomes
  much more valuable

### Stage 8: Add the SSH Console

Implement SSH as the preferred secure human interface:
- same command engine as telnet
- no shell
- callsign/password first
- key auth later
- ports likely `7322` and/or `2222`

Why after Stage 4:
- SSH should use the same privilege/access model
- avoids designing access controls twice

### Stage 9: Federated Identity and Profile Sync

Implement pyCluster-native federation:
- home-node ownership
- password/profile sync
- `registered_at` oldest-wins semantics
- `updated_at` newest-from-home semantics
- expiry/lease model
- trusted-node propagation

Why late:
- this depends on settled user data, access policy, and operator tooling
- avoids redesigning federation records as local account handling evolves

### Stage 10: Enrichment and Optional External Data

Add richer lookup sources where useful:
- FCC ULS local cache/import for US calls
- expanded CTY usage
- optional future enrichment sources

Why last:
- low risk to add later
- should plug into an already stable user/profile model

## Near-Term Working Order

The next practical sequence should be:

1. stabilize runtime issues that block development
2. finish telnet UX cleanup
3. add `sysop/` namespace
4. implement access matrix
5. add public web auth/posting
6. add `show/links` and topology/version visibility
7. add audit trail
8. add SSH console
9. add federation
10. add external enrichment
