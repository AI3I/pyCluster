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
- `Operator Tools`
- `Telemetry`

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

`Public IPv4 Address` and `Public IPv6 Address` are used when pyCluster sends outbound PC92 path data. If a PC92 payload would otherwise advertise a private, loopback, link-local, `localhost`, or otherwise non-public IP literal, pyCluster substitutes the same-family configured public address before sending the frame to peers. Blank fields are auto-populated in the SysOp form from detected global interface addresses when available; saving persists the displayed values. The core service also uses detected global interface addresses as a runtime fallback for PC92 sanitization and PC61 spot relay when the saved fields are blank.

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

### Mail (SMTP)

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

### Reverse Beacon Network

- `Enable RBN Feed`
- public feed toggles for `CW/RTTY` and `FT8`
- feed status
- `Advanced Options`

Direct RBN ingestion is disabled by default. When enabled from the disabled state, `CW/RTTY` is selected by default and `FT8` is left disabled unless the sysop enables it. Changes made while RBN is enabled are honored. `Advanced Options` contains host, default port, feed ports, named feeds, login callsign, feed password, source node, startup commands, and reconnect seconds.

RBN spots still respect the per-user and peer access matrix. Telnet users do not receive live RBN spots by default; they opt in with `set/rbn`. `show/rbn` remains available for summarized RBN history.

## Users

The `Users` view is a browser-first workspace. Click on a user to edit account details. Existing users open in a modal editor, and `New User` opens the same modal for a new account.

Browser tabs:

- `Users`
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

- `Dial-out`
- `Accepted`

Meaning:

- `Dial-out`: this node initiates the connection and can retry it
- `Accepted`: the remote node connects inbound, so no local DSN or retry is used

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
- `Role`
- `Status`
- `Traffic`
- `Health`

The `Role` cell now carries:

- peer family
- retry mode
- host / transport endpoint
- learned peer software/version when it has been seen from `PC18`

The `Status` cell answers the simple transport question first:

- `connected`: a live socket exists
- `disconnected`: no live socket exists

The detail text then describes recent activity:

- `bidirectional`
- `receive active`
- `transmit active`
- `idle`
- `connected quiet`

This is intentionally separate from protocol-health freshness. A connected inbound node can be `transmit active` and `receive quiet` when pyCluster is still sending keepalives or traffic but has not recently received PC protocol frames from the remote node. In that case, the transport is connected; the protocol-health view is where stale/degraded/flapping protocol state is investigated.

This view is intended to make peer operations understandable without dropping into raw counters or logs.

## Protocol Health

This view focuses on peer state, alerting, and protocol history.

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

#### Protocol Alerts

Columns:

- `Peer`
- `Health`
- `Age`
- `Flap`
- `Status`

#### Policy Drops

Columns:

- `Peer`
- `Total`
- `Loop Drops`
- `Reasons`

#### Protocol History

Columns:

- `Peer`
- `When`
- `Key`
- `From`
- `To`

This area is the main operator view for peer health and protocol instability.

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
