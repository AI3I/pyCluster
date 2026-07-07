# User Manual

This manual is for ordinary pyCluster users connecting through telnet or the public web UI.

It is meant to explain how to use the cluster, not how to administer it.

## 1. Logging In

pyCluster users can work with the system through:

- telnet
- the public web UI

### Telnet Login

Typical flow:

- connect to the node
- enter your callsign
- enter your password
- enter an MFA code if your account requires one

For first-time human telnet users:

- if no password exists yet, pyCluster will require password creation before continuing

For cluster-peer/node records:

- node-classified records do not use the ordinary human password prompt path

Typical successful telnet session:

```text
login: N0CALL
password:
Welcome to pyCluster on N0CALL-1
N0CALL-1>
```

If MFA is required, telnet prompts for `authenticator code:` when your account uses an app, or `otp:` when your account uses email codes. Password and MFA-code entry are not echoed back to the terminal.

For SSID logins, self-service MFA commands apply to the exact callsign you used to log in. For example, `set/mfa authenticator` as `N0CALL-2` enrolls MFA for `N0CALL-2`, not for the base `N0CALL` account.

If this is your first telnet login and no password exists yet, pyCluster will stop and require password creation before continuing.

If your node requires or encourages registration, telnet shows a registration
notice after the MOTD until your account has an approved registration request.
Self-registration is limited to simple ham-style callsigns with an optional
numeric SSID, such as `N0CALL` or `N0CALL-2`. Portable/slashed forms are still
valid in spot traffic, but should not be used as the registration principal.
Use the normal profile commands to fill in missing fields, then run:

```text
register
```

`register` submits the current profile to the same pending registration queue
used by the public web UI. If SMTP is configured, pyCluster first sends a
verification code to your profile email address. Enter `REGISTER <code>` to
verify the email address and queue the sysop review request. If the code
expires, run `REGISTER` again to request a fresh code.

## 2. Getting and Viewing DX

The main command is:

```text
sh/dx
```

Use `sh/dx` when you want the traditional global cluster view of recent human-posted DX spots. Personal accept/reject spot filters do not hide results from this history view; use `sh/mydx` when you want the same style of list after applying your filters. RBN/Skimmer reports are kept out of `sh/dx`; use `show/rbn` for recent RBN history.

It is the fastest way to:

- see what has been active recently
- search for a specific DX callsign
- narrow by band or spotter
- search comments for mode or activity text

Useful examples:

```text
sh/dx 10
sh/dx K9JR exact
sh/dx by WW5L
sh/dx on 40m
sh/dx info RTTY
sh/dx day 2
sh/mydx 10
```

Meaning:

- `sh/dx 10`
  - show the latest 10 spots
- `sh/dx K9JR exact`
  - show spots specifically for `K9JR`
- `sh/dx by WW5L`
  - show spots made by `WW5L`
- `sh/dx on 40m`
  - limit to 40 meters
- `sh/dx info RTTY`
  - search spot comments for `RTTY`
- `sh/dx day 2`
  - search the recent two-day window

Related views:

- `show/dx`
- `show/prefix <prefix>`
- `show/lastspot <call>`
- `show/qrz <call>` if QRZ XML lookup is configured on the node
- `show/dxstats`
- `show/hfstats`
- `show/vhfstats`

Personal spot filters are applied to incoming spot traffic and to `sh/mydx`. Spotter filters match the station that posted the spot, which is usually what matters when deciding whether a local station can realistically hear the same DX.

## 3. Personal Profile

Users can maintain their own station/profile details.

Common commands:

```text
set/name Example Operator
set/qth Example City
set/qra FN31PR
set/email john@example.net
set/homenode N0CALL-1
set/password mynewpass
mfa
set/mfa authenticator
set/mfa email
unset/mfa
```

These profile commands are local to the node unless future federation features say otherwise.

In practice:

- `set/name`
  - stores your operator name
- `set/qth`
  - stores your location text
- `set/qra`
  - stores your grid or QRA-style field
- `set/homenode`
  - stores the node that is considered your home
- `set/password`
  - changes your local password on this node
- `set/mfa authenticator`
  - enrolls an authenticator-app secret for login codes
- `set/mfa email`
  - uses email one-time codes when SMTP and a profile email are available
- `unset/mfa`
  - disables user-level MFA and clears outstanding challenges

Useful related views:

- `show/station`
- `show/registered`

After changing your details, it is normal to verify them with:

```text
show/station
show/registered N0CALL
```

## 4. Session Preferences

Users can adjust how the telnet interface behaves.

Common commands:

```text
set/page 20
set/language de
set/echo
unset/echo
set/beep
unset/beep
set/nowrap
unset/nowrap
```

Important notes:

- `set/page 20`
  - makes long listings easier to read on a narrow terminal
- `set/nowrap`
  - disables the default wrapped formatting
- `unset/nowrap`
  - restores the more conservative default style

## 5. Mail, Talk, and Announcements

pyCluster supports message-style and bulletin-style traffic.

Examples:

```text
send NO3M Hello from pyCluster
msg NO3M Hello from pyCluster
read 12
reply 12 Copy, thanks
talk K9JR Good evening
announce Club meeting tonight
```

When to use each:

- `send` or `msg`
  - send a stored message to another user
- `read`
  - read a stored message by number
- `reply`
  - respond to a stored message
- `talk`
  - send chat-style traffic
- `announce`
  - send announcement traffic to the cluster

Useful views:

- `show/messages`
- `show/mail`
- `show/outbox`
- `show/msgstatus`
- `show/chat`
- `show/announce`
- `show/wcy`
- `show/wwv`
- `show/wx`

Message delivery states:

- `local`
  - stored on this node with no remote routing needed
- `delivered`
  - delivered to a currently connected local session or received from a linked peer
- `pending`
  - queued for a remote home node that is not connected yet
- `routed`
  - handed to the linked node for onward delivery

## 6. Filters

Users can filter the information they receive.

Examples:

```text
accept/spots 20m
reject/spots FT8
accept/spots call_zone 5
accept/spots call_dxcc canada
clear/spots
set/rbn
unset/rbn
accept/rbn 1 call N9JR
reject/rbn 2 info TEST
clear/rbn
show/rbn
show/rbn N9JR 20
```

Inspect filters with:

```text
show/filter
```

Filters are useful when:

- a band or mode is too noisy
- you only want certain activity types
- you want special handling for RBN-style spot traffic
- you want recent Skimmer reports showing which stations are hearing a callsign
- you want entity-aware spot filtering by CQ zone or DXCC entity name/prefix
- you want spotter-based filtering so `sh/mydx` reflects spots posted from places you can reasonably use

RBN live delivery is opt-in for telnet users. Run `set/rbn` when you want RBN/Skimmer reports in your live stream, and `unset/rbn` to turn them off. `show/rbn` remains available for recent RBN history even when live delivery is off.

`accept/rbn` and `reject/rbn` are separate RBN filter-family commands. For example, `accept/rbn 1 call N9JR` allows only RBN/Skimmer spots whose spotted DX call matches `N9JR`; it does not affect ordinary human-posted spots. `reject/rbn 2` remains a shorthand for rejecting all RBN spots in slot 2.

Legacy rules such as `accept/spots 1 rbn call N9JR` are interpreted as RBN-scoped rules. When any RBN-scoped rule exists, ordinary `accept/spots ... by ...` rules do not open the full automated RBN stream; general reject rules still apply to both sources.

RBN output is summarized before display. Instead of listing every raw Skimmer line, pyCluster shows mode, signal level, skimmer count, and CQ-zone summary in a compact form such as `CW 8dB Q:9* Z:3,4,5`.

When the node operator configures a direct RBN-enabled telnet feed, pyCluster can also ingest those Skimmer reports locally. The public RBN relays are `telnet.reversebeacon.net:7000` for CW/RTTY and `telnet.reversebeacon.net:7001` for FT8; configure both with named feeds such as `CW/RTTY,telnet.reversebeacon.net,7000` and `FT8,telnet.reversebeacon.net,7001` in SysOp, or with `feeds` in config. If no direct feed is configured, RBN spots can still arrive through linked cluster peers that relay them.

## 7. Buddy Lists and User Data

Buddy list examples:

```text
set/buddy NO3M
unset/buddy NO3M
show/buddy
```

User-variable examples:

```text
set/var color blue
show/var
unset/var color
```

Buddy lists are useful for keeping an eye on operators or stations you care about.

User variables are useful for local preference-like data that does not belong in your core station profile.

## 8. Public Web UI

The public web UI gives users a browser interface for:

- viewing live spots
- browsing announcements, chat, WX, WCY, and WWV bulletins
- cluster summary
- logging in from the footer `Log In` modal
- registration requests from the footer `Register` modal
- watch rules after login
- posting after login
- editing their profile

Logged-in public web filters are stored in the same database-backed filter table used by telnet. Common filter combinations are saved as a compound `accept/spots` rule so the web and telnet views make the same filtering decision for that user. The public web exposes band, mode, activity, continent, CQ-zone, spotter-continent, spotter-CQ, and comment-tag filters; ITU-zone filters are not exposed in the web UI.

The profile modal lets logged-in users update name, QTH, grid square, home node, and email address. It also exposes MFA settings for switching between email and authenticator-app codes, enabling an app with a QR code, verifying the active method, and disabling user-level MFA.

If the browser does not already have a local QTH override, the spot map seeds its QTH marker from the logged-in profile grid square.

See:

- [Public Web UI](public-web.md)

In general, the public web UI is more convenient when you want:

- visual filtering
- cluster overview tables
- watch rules and recent matches after login
- profile editing without typing commands

## 9. Command Discovery

Useful discovery commands:

```text
help
show/commands
show/shortcuts
apropos route
```

See also:

- [Telnet Commands](telnet-commands.md)
- [Telnet Command Reference](telnet-command-reference.md)

Good first-session workflow:

```text
help
show/commands
sh/dx 10
show/station
set/name Your Name
set/qth Your Location
set/qra Your Grid
```
