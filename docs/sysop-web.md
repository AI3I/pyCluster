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

### Mail (SMTP)

- `SMTP Host`
- `SMTP Port`
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

## Users

The `Users` view is a single workspace with a browser area above the editor.

Browser tabs:

- `Local Users`
- `Blocked Users`
- `Clusters`
- `System Operators`
- `Requests`

### User Details

This is the main user editor.

Fields:

- `Callsign`
- `Access Level`
- `Name (QRA)`
- `Location (QTH)`
- `Grid Square`
- `Email`
- `Password`
- `Home Node`
- `Notes / Block Reason`

Actions:

- `New User`
- `Update User`
- `Set Password`
- `Remove User`
- `Send Verification`
- `Reset MFA`

Password behavior:

- normal password text sets or changes the password
- entering `CLEAR` and then `Set Password` clears it

Access levels:

- `none`
- `user`
- `sysop`
- `blocked`

Default behavior by level:

- `none`: login, chat, WX, WCY, and WWV remain allowed by default; spot and announce posting are off until access is explicitly granted
- `user`: normal login and posting access
- `sysop`: sysop login plus full administrative access
- `blocked`: login denied for the base callsign and matching SSIDs

If `Blocked` is selected:

- login is blocked for the base callsign and matching SSIDs
- the notes field also serves as the block reason

### Access Matrix

Per-user policy matrix.

Channels:

- `TELNET`
- `WEB`

Capabilities:

- `Login`
- `Spots`
- `Chat`
- `Announce`
- `WX`
- `WCY`
- `WWV`

Actions:

- `Add All`
- `Remove All`

This is the operational source of truth for where a user may log in and what they may post.

Explicit access-matrix overrides take precedence over the default behavior implied by the selected access level.

This section also carries:

- `Access Level`
- `Email MFA Override`
- `Verified`
- `Locked`

`Verified` and `Locked` are read-only state indicators in the matrix. They are not separate editor buttons.

The `Clusters` browser tab shows any user record with a real cluster-node family such as `pycluster` or `dxspider`.

## Peers and Links

This view manages node-link peers and shows live link state.

### Peer Editor

Main fields:

- `Peer Name`
- `Transport Address`
- `Cluster Family`
- `Peer Password (Optional)`
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
- `Refresh`
- `Connect`
- `Disconnect`

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
- `WCY`
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
- `Recent Auth Failures`

Recent auth failures show:

- when
- channel
- IP
- callsign
- reason

Current bans show:

- `fail2ban` jail
- IP

This area is the main operator-facing view for login abuse and automatic bans.

## User and Login Visibility

The user-management tables now surface recorded login path data directly in the table view.

Visible columns now include:

- `Last Path` for `System Operators`
- `Last Path` for `Blocked Users`
- `Last Path` for `Local Users`

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
