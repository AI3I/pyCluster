# System Operator Web

The System Operator web console is the browser-based control surface for a pyCluster node.

Default local URL:

- `http://127.0.0.1:8080/sysop/`

This UI is meant to be the day-to-day operator workspace, not just a diagnostics page.

## Login

Use a local callsign that has:

- a configured password
- `System Operator` access on this node

The login page gives explicit feedback for:

- bad password
- blocked callsign
- web login denied by policy

## Global Layout

The console is organized into these main views:

- `Node Settings`
- `Users`
- `Peers and Links`
- `Protocol Health`
- `Topology`
- `Operator Tools`
- `Telemetry`
- `Taxonomy`

The left sidebar also includes:

- `At A Glance`

The masthead includes:

- current operator session state
- `Refresh Console`
- theme toggle

On phone, tablet, and foldable layouts, the console uses mobile-specific
breakpoints:

- main navigation and subtab rows wrap into responsive grids
- the Users browser tabs stay visible without horizontal overflow
- action rows and registration/account controls expand to full-width touch targets
- large tables remain horizontally scrollable instead of wrapping into unreadable cells

## Node Settings

This view controls local node identity and welcome-flow presentation.

Tabs:

- `General`
- `Authentication`
- `SMTP`
- `QRZ Lookup`
- `Satellite`
- `RBN`
- `pyCluster Protocol`
- `Maintenance`

### Main Fields

- `Node Call / SSID`
- `Node Alias`
- `Owner Name (QRA)`
- `Location (QTH)`
- `Grid Square`
- `Telnet Ports`
- `Node Brand`
- `Welcome Title`
- `Website URL`
- `Support Contact`
- `Public IPv4 Address`
- `Public IPv6 Address`

`Public IPv4 Address` and `Public IPv6 Address` are used when pyCluster sends outbound peer traffic. If a PC92 payload would otherwise advertise a private, loopback, link-local, `localhost`, or otherwise non-public IP literal, pyCluster substitutes the same-family configured public address before sending the frame to peers. Locally generated PC61 spot and PC93 chat/bulletin relays use the same public-address selection. Blank fields are auto-populated in the SysOp form from detected global interface addresses when available; saving persists the displayed values. The core service also uses detected global interface addresses as a runtime fallback when the saved fields are blank.

### Long-Text Fields

- `Welcome Body`
- `MOTD`
- `Login Tip`

### Behavior Flags

- `Show node status after MOTD`
- `Require telnet password on fallback paths`

### Authentication

- `Require registration for users`
- `Require verified email for web`
- `Require verified email for telnet`
- `Registration Grace Logins`
- `Enable MFA login challenges`
- `Require MFA challenge for sysop logins`
- `Require MFA challenge for user logins`
- `MFA Issuer`
- `OTP TTL (seconds)`
- `OTP Length`
- `OTP Attempts`
- `OTP Resend Cooldown (seconds)`

Per-user MFA actions:

- `Enroll Authenticator` creates a Google Authenticator-compatible TOTP secret for the selected principal callsign and displays the setup material.
- `Reset MFA` disables user-level MFA, clears outstanding email OTP challenges, and removes any authenticator secret.

Users can also manage their own MFA from the public web profile popup or from telnet with `mfa`, `set/mfa`, `set/totp`, `unset/totp`, and `unset/mfa`. Node-wide MFA defaults are applied only after a user has usable MFA material, such as a valid email address or authenticator secret. Web login prompts describe the code source as email or authenticator app without exposing internal method names.

Pending registration requests older than 24 hours are highlighted in the Requests view and summarized above the request table. This dashboard warning does not approve or deny anything. Separately, `pycluster-registration-reminders.timer` emails each applicant whose request remains pending at 1, 4, 7, 10, and 14 days. Reminder stages are persisted, missed intervals are not sent in a burst, and automatic mail stops after day 14.

### SMTP

- `SMTP Host`
- `SMTP Port`
  - `Submission / STARTTLS (587)`
  - `Implicit TLS / SMTPS (465)`
  - `SMTP (25)`
- `SMTP Username`
- `SMTP Password`
- `From Email`
- `From Name`
- `Use STARTTLS`
- `Use SSL`
- `Send MFA Test Email`

### Main Action

- `Save Node Settings`

This section controls:

- telnet welcome flow
- node presentation shown to users
- branding and contact metadata used by the public-facing web experience

### RBN

- `Enable RBN Feed`
- public feed toggles for `CW/RTTY` and `FT8`
- feed status
- `Advanced Options`

Direct RBN ingestion is disabled by default. When enabled from the disabled state, `CW/RTTY` is selected by default and `FT8` is left disabled unless the sysop enables it. Changes made while RBN is enabled are honored. `Advanced Options` contains host, default port, feed ports, named feeds, login callsign, feed password, source node, startup commands, and reconnect seconds.

RBN spots still respect the per-user and peer access matrix. Telnet users do not receive live RBN spots by default; they opt in with `set/rbn`. `show/rbn` reports bounded current-session summaries. RBN reports are not retained in SQLite.

Feed status and the At A Glance RBN summary refresh every five seconds while the
authenticated console is visible. This refresh does not change unsaved form
values. A failed status request shows Unavailable rather than retaining an old
connection state. The QRZ Agent placeholder shows the current runtime default;
leaving the actual field blank keeps version selection automatic across upgrades.

### QRZ Lookup

This tab configures the optional QRZ XML credentials, API endpoint, and agent string used by `show/qrz`.

### pyCluster Protocol

This tab controls the optional private PY protocol used only between authenticated peers that identify as pyCluster and negotiate compatible capabilities. It contains:

- the global PY enable switch
- node information and known-node topology sharing
- health, dataset, RBN, access-policy, clock, and uptime sharing
- explicit public URL, locator, QTH, and System Operator contact privacy controls
- the shared metadata preview
- structured network-notice sharing

The controls are local policy settings. Existing pyCluster links must reconnect to negotiate newly enabled capabilities. Live negotiation state and alerts remain under `Protocol Health`; the known-node catalog and route provenance are under `Topology`.

### Maintenance

The Maintenance action row contains `Save Node Settings`, `Run Cleanup Now`, `Check for Upgrade`, and `Run Upgrade`. `Check for Upgrade` compares the running version with semantic-version tags advertised by the configured Git origin. **Latest remote tag** is the release available from origin; **Cached source tag** is the newest tag already present in the local source checkout and may lag until a fetch or lifecycle run refreshes it.

`Run Upgrade` writes a request for the root-owned `pycluster-upgrade.path` watcher. Because that worker executes deployment code as root, the action requires the source root, its `.git` directory, and `deploy/upgrade.sh` to be root-owned and not group/world-writable. It is also disabled when the source checkout has local changes. The worker independently checks both conditions before it fetches release tags, selects only a newer semantic-version release, checks out that tag, and invokes the same `deploy/upgrade.sh` path used from a shell. Upgrade and repair stop live writers, create a timestamped preflight backup, preserve runtime configuration/data/log directories during code synchronization, and require every configured telnet and local web health endpoint to respond before reporting success. If maintenance fails, services that were active are restarted and the backup remains available for deliberate recovery; database restoration is not attempted automatically.

## Users

The `Users` view is a browser-first workspace. Click on a user to edit account details. Existing users open in a modal editor, and `New User` opens the same modal for a new account.

Browser tabs:

- `Users`
- `Blocked`
- `Locked`
- `Clusters`
- `System Operators`
- `Requests`

The `Users` table is the master matrix for local account state and access. It shows touch-friendly toggles for:

- `Verified`
- `Locked`
- `MFA`
- `Blocked`
- `Login`
- `Spots`
- `RBN`
- `Chat`
- `Annc`
- `WX`
- `WCY`
- `WWV`

### User Details

This modal is the main user editor.

Fields:

- `Callsign`
- `User Type`
- `Name (QRA)`
- `Location (QTH)`
- `Grid Square`
- `Home Node`
- `Email`
- `Password`
- `Notes / Block Reason`

User type values:

- `Standard User`
- `System Operator`
- `Cluster > pyCluster`
- `Cluster > DXSpider`
- `Cluster > DxNet`
- `Cluster > AR-Cluster`
- `Cluster > CLX`

Actions:

- `Update User`
- `Remove User`
- `Set Password`
- `Send Verification`
- `Unlock Account`
- `Enroll Authenticator`
- `Reset MFA`
- `Close`

Password behavior:

- normal password text sets or changes the password
- entering `CLEAR` and then `Set Password` clears it

Status cards in the modal include:

- online state
- last login
- last path
- inbox and outbox counts
- login access
- posting access by `Telnet` and `Web`
- MFA state
- registration state

`Remove User` asks for confirmation before deleting the record. Cluster peer records are treated as verified and unblocked, have MFA off, and have all access enabled because they are manually provisioned node identities rather than self-service human accounts.

When the `Locked` state is checked, `Unlock Account` clears the durable account lock, resets the failed-password counter, and preserves an already verified email address.

### Access Matrix

Per-user policy matrix.

Channels:

- `TELNET`
- `WEB`

Capabilities:

- `Login`
- `Spots`
- `RBN`
- `Chat`
- `Annc`
- `WX`
- `WCY`
- `WWV`

This is the operational source of truth for where a user may log in and what they may post.

The row matrix is editable from the table. The modal status summary shows the same posting permissions as `Yes` and `No` pills for quick review.

The `Clusters` browser tab shows any user record with a real cluster-node family such as `pycluster`, `dxspider`, `dxnet`, `arcluster`, or `clx`.

## Peers and Links

This view manages node-link peers and shows live link state.

### Peer Editor

Main fields:

- `Peer Name`
- `Peer Password`
- `Transport Address`
- `Cluster Family`
- `Retry Automatically`

Peer password note:

- some peer operators require a password for node-to-node login
- the password is optional
- it should be coordinated with the remote peer operator

### Roles

Peer rows distinguish:

- `Outbound`
- `Inbound`

Meaning:

- `Outbound`: this node initiates the connection and can retry it
- `Inbound`: the remote node connects inbound, so no local DSN or retry is used

### Main Actions

- `New Peer`
- `Save Peer`
- `Delete Peer`
- `Close`
- `Refresh`
- `Connect`
- `Disconnect`

`New Peer`, peer-row selection, and saved-peer editing use a modal editor. `Delete Peer` asks for confirmation before removing the saved peer. Live `Connect` and `Disconnect` remain on the status page.

`Disconnect` closes the live link but keeps the saved peer target in the table as `disconnected`, so it can be selected and reconnected later. `Connect` can use the saved transport address for the selected peer even when the form transport field is empty.

### Peer Table

Columns:

- `Peer`
- `Connection`
- `Activity`
- `Operations`

The `Peer` cell carries the configured or observed peer family.

The `Connection` cell answers the simple transport question first:

- `connected`: a live socket exists
- `disconnected`: no live socket exists

It also shows inbound/outbound direction and the most recent connection error when applicable. The `Activity` cell shows the last receive/transmit times. `Operations` keeps retry and queued-mail state together.

Detailed transport addresses and connection errors remain in the peer modal. Frame counts, PC/PY state, version advertisements, rejected frames, and protocol analysis live on the separate Protocol Health page.

This view is intended to make peer operations understandable without dropping into raw counters or logs.

## Protocol Health

Address-level connection policy is managed in **Telemetry > Security > Address
Blocks**. It supports IPv4/IPv6 CIDRs, expiration, and retained removal history.
The default **Active Blocks** view excludes removed and expired entries. Select
**History** to see those inactive records; they no longer block traffic. Adding
a block returns the table to Active Blocks.
See [Security](security.md) for proxy configuration and enforcement scope.

This view focuses on live peer state, negotiated protocol metadata, alerting, and protocol history. PY sharing policy and notices are configured under `Node Settings > pyCluster Protocol`.

### Threshold Fields

- `Stale Minutes`
- `Flap Score`
- `Flap Window Seconds`
- `History Limit`

### Main Actions

- `Save Thresholds`
- `Reload History`
- `Reset Protocol History`
- `Reset Policy Drops`

### Summary Cards

- `Peers`
- `Healthy`
- `History`
- `Alerts`

### Main Tables

#### Peer State

Separates each peer into `Connection`, `PC Protocol`, `PY Protocol`, and
`Activity`. PY states report direct evidence such as `Disabled locally`,
`PY00 sent; no response`, `Invalid PY00 response`, `Negotiated; awaiting
NODEINFO`, and `Negotiated`.

Peer State also reports session-scoped PY receive/transmit byte totals,
rejected-frame counts, and a conformance verdict explaining the current
negotiation outcome.

#### Connection Alerts

Columns:

- `Peer`
- `Health`
- `Age`
- `Flap`
- `Status`

#### Rejected Frames

Columns:

- `Peer`
- `Total`
- `Loop Drops`
- `Reasons`

This table contains frames that arrived and were rejected by local policy. A
silent peer that does not answer `PY00` is not a drop; that evidence appears in
Peer State so the failed negotiation remains visible for troubleshooting.

#### Protocol History

History spans the full content width. Connection Alerts and Rejected Frames
appear beneath it in equal-width columns, stacking on smaller screens.

Use the `PY`, `PC`, and `All` tabs to separate event families.
The configured history limit applies to the selected family.
Family selection is applied by the API before limiting results, so newer PC
events cannot hide an older retained PY negotiation event. History values retain
their original case.

Columns:

- `Peer`
- `When`
- `Key`
- `From`
- `To`

This area is the main operator view for peer health and protocol instability.

## Topology

### Peer Drafts And Paging

Topology defaults to 15 nodes per page and supports page-size selection and
search by callsign, UUID, locator, or QTH. Export still includes the full catalog.
Open a node's route details and choose Add Peer to open the local peer editor.
Existing matching peer records are reopened instead of creating a second record.
The local node cannot be added as its own peer.

New drafts contain the advertised callsign and pyCluster family, with provenance
shown separately. They leave transport and credentials blank and automatic retry
off. A public web URL is not a verified telnet endpoint and is never converted
into one. Review the endpoint and authentication before saving; enabling retry
is an explicit operator choice. No request or configuration is sent to the
remote node by this workflow.

Reported health now includes the services marked down by a remote node. In
split-process installations, the core cannot infer public-web health from its
own process state; it reports that component locally as unknown and omits it
from PY health rather than claiming it is down. This is not an external health
probe. Older peers can continue sending the false down report until upgraded.

### Known pyCluster Nodes

The dedicated Topology view uses four wrapping columns: node, identity,
path and services, and freshness. Locator and QTH appear with the node UUID
because they describe node identity rather than route state. On narrow screens each node becomes
a labeled stacked record instead of forcing the console to scroll horizontally.

This table includes two levels of knowledge:

- a direct peer positively identified as pyCluster by its PC18 software string,
  even when private-protocol negotiation has not completed
- direct or reported node records received through negotiated `PY01` and
  topology exchange

An identified-only row explicitly reports whether PY is disabled locally,
is awaiting a valid PY00 handshake, or has negotiated but not received NODEINFO.
The PC18 software version is informational and never substitutes for PY00
capability negotiation. The row reports whether PY00 was not sent, sent without
a valid response, followed by a disconnect, or whether an invalid or
identity-mismatched PY00 was received. A silent peer cannot be conclusively
labeled remotely disabled or rejecting PY.
A negotiated row can also report that NODEINFO
has not been received. Locator, services, public URL, health, dataset, RBN, and
topology metadata remain unavailable until both nodes enable compatible `PY`
capabilities and exchange the corresponding records.

PY v2 peers also expose their negotiated frame, record, and hop limits, current
probe responsiveness and RTT, and topology synchronization state. Origin-owned
direct-neighbor lists support the reported network graph; clean withdrawals
remove owned routes promptly and abrupt failures remain bounded by record expiry.
The catalog retains bounded alternate routes and promotes a live alternate when
the preferred path disappears. Route counts, one-sided links, and neighbors not
yet present in the catalog are shown as diagnostic context, not authoritative
network faults.
Select a route count to inspect the selected and alternate live routes, including
their learned-from peer, source, confidence, hop count, sequence, last-seen time,
and lease expiry. This detail is available only to authenticated System Operators.

`Export JSON` downloads a sanitized, versioned snapshot of
the live known-node catalog and retained routes for troubleshooting; it does
not include transport addresses, credentials, tokens, users, or configuration.

Lists local, directly observed, and relayed pyCluster node records with version, location, provenance, services, and freshness. These records are reported topology observations, not a central registry.

## Operator Tools

This view is for authenticated operator posting.

### Spot Fields

- `DX Call`
- `Frequency kHz`
- `Spot Info`

### Message Fields

- `Announce Scope`
- `Message / Bulletin Text`

### Actions

- `Post Spot`
- `Chat`
- `Announce`
- `WWV`
- `WX`

This uses the current logged-in sysop identity.

## Telemetry

This view groups runtime visibility and operational history.

### Runtime Stats

Cards:

- `Node`
- `Uptime`
- `Stored Spots`
- `Telnet Sessions`
- `Web Sessions`

### Recent Spots

The RBN view uses a bounded in-memory sample of the latest 200 accepted RBN
spots, including direct feeds and peer-delivered RBN reports. The All view merges
that sample with stored cluster spots. The sample resets on restart; it does not
restore RBN database persistence. Feed connection state remains under Node
Settings > RBN.

Columns:

- `Frequency`
- `DX`
- `When`
- `Spotter`
- `Info`
- `Node`

### Recent Audit

Includes:

- category filter
- reload button

Categories currently exposed:

- `System Operator`
- `User`
- `Config`
- `Control`
- `Connect`
- `Disconnect`

### Audit

Includes:

- `Current Bans`

### Security

Includes:

- `Reload Security`
- `Recent Authentication Failures`
- `Recent Logins`

Recent auth failures show:

- when
- channel
- IP
- callsign
- reason

Recent logins show:

- when
- callsign
- display name
- role
- recorded path

Current bans show:

- `fail2ban` jail
- IP

This area is the main operator-facing view for login abuse and automatic bans.

## User and Login Visibility

The user-management tables now surface recorded login path data directly in the table view.

Visible columns now include:

- `Last Path` for `System Operators`
- `Last Path` for `Users`
- `Last Path` for `Clusters`

This is intended to expose the recorded interface, source address, listener port, and related path detail without requiring hover-only inspection.

## Operational Notes

- the console is meant to reduce the need for direct database edits or log-tail-only operations
- most actions write to the same underlying state used by the telnet command surface
- the UI tries to show actual operator intent instead of raw internal values where possible

## Relationship to Telnet

The sysop web console does not replace telnet. It complements it.

Use the web console for:

- structured editing
- runtime visibility
- peer and user management

Use telnet when you want:

- command-line workflows
- quick operator actions
- traditional cluster interaction
