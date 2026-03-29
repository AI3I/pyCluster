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
- `Require telnet passwords for users`

### Main Action

- `Save Node Settings`

This section controls:

- telnet welcome flow
- node presentation shown to users
- branding and contact metadata used by the public-facing web experience

## Users

The `Users` view is split into several operational sections.

### System Operators

Shows local sysops and where they are active.

Columns:

- `Callsign`
- `Name`
- `Email`
- `Telnet`
- `Web`

Active sessions are shown as:

- `now`

Recent activity is shown relatively, for example:

- `19h ago`

User last-login details now carry normalized inbound path information, and live peer rows include transport/path hints when pyCluster has them.

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
- `Save User`
- `Set Password`
- `Remove User`

Password behavior:

- normal password text sets or changes the password
- entering `CLEAR` and then `Set Password` clears it

Access levels:

- `Non-Authenticated`
- `Authenticated`
- `System Operator`
- `Blocked`

Default behavior by level:

- `Non-Authenticated`: login, chat, WX, WCY, and WWV remain allowed by default; spot and announce posting are off until access is explicitly granted
- `Authenticated`: normal login and posting access
- `System Operator`: sysop login plus full administrative access
- `Blocked`: login denied for the base callsign and matching SSIDs

If `Blocked` is selected:

- login is blocked for the base callsign and matching SSIDs
- the notes field also serves as the block reason

### Advanced Node Login

Collapsed advanced section for node-to-node login identity.

Field:

- `Cluster Node Family`

Options:

- `Not a cluster peer`
- `pyCluster`
- `DXSpider`
- `DxNet`
- `AR-Cluster`
- `CLX`

This should only be used for cluster-peer records, not ordinary human users.

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

### Blocked Users

Separate full-width table for blocked local users.

Shows:

- callsign
- access
- home node
- block reason
- blocked time

Blocked users are intentionally not mixed into the ordinary local-user list.

### Local Users

Full-width table of ordinary local users who are not sysops and are not blocked.

This section is meant to be the normal operating user view.

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
- `Connect`
- `Disconnect`
- `Reset Policy Drops`

### Peer Table

Columns:

- `Peer`
- `Role`
- `Status`
- `Family`
- `Traffic`
- `Policy Drops`
- `Health`

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
- `Reset Proto History`

### Summary Cards

- `Tracked Peers`
- `Healthy`
- `Alerts`
- `Acknowledged`

### Main Tables

#### Protocol Summary

Columns:

- `Peer`
- `Health`
- `Age`
- `Changes`
- `Flap`
- `Last Event`

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

### Security

Includes:

- `Reload Security`
- `Recent Auth Failures`
- `Current Bans`

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
