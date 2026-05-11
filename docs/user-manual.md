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

If this is your first telnet login and no password exists yet, pyCluster will stop and require password creation before continuing.

## 2. Getting and Viewing DX

The main command is:

```text
sh/dx
```

Use `sh/dx` when you want the traditional global cluster view of recent spots. Personal accept/reject spot filters do not hide results from this history view; use `sh/mydx` when you want the same style of list after applying your filters. The RBN display toggle is still honored, so `unset/rbn` hides RBN/Skimmer spots from both views.

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
accept/spots call_itu 9
accept/spots call_dxcc canada
clear/spots
accept/rbn CQ
reject/rbn TEST
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
- you want entity-aware spot filtering by CQ zone, ITU zone, or DXCC entity name/prefix
- you want spotter-based filtering so `sh/mydx` reflects spots posted from places you can reasonably use

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

The profile modal lets logged-in users update name, QTH, grid square, home node, and email address. It also exposes MFA settings for switching between email and authenticator-app codes, enabling an app with a QR code, verifying the active method, and disabling user-level MFA.

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
