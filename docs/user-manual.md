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

For first-time human telnet users:

- if no password exists yet, pyCluster will require password creation before continuing

For cluster-peer/node records:

- node-classified records do not use the ordinary human password prompt path

Typical successful telnet session:

```text
login: AI3I
password:
Welcome to pyCluster on AI3I-16
AI3I-16>
```

If this is your first telnet login and no password exists yet, pyCluster will stop and require password creation before continuing.

## 2. Getting and Viewing DX

The main command is:

```text
sh/dx
```

Use `sh/dx` when you want the traditional cluster view of recent spots.

It is the fastest way to:

- see what has been active recently
- search for a specific DX callsign
- narrow by band or spotter
- search comments for mode or activity text

Useful examples:

```text
sh/dx 10
sh/dx K3AJ exact
sh/dx by WW5L
sh/dx on 40m
sh/dx info RTTY
sh/dx day 2
```

Meaning:

- `sh/dx 10`
  - show the latest 10 spots
- `sh/dx K3AJ exact`
  - show spots specifically for `K3AJ`
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
- `show/qrz <call>`
- `show/dxstats`
- `show/hfstats`
- `show/vhfstats`

## 3. Personal Profile

Users can maintain their own station/profile details.

Common commands:

```text
set/name John Lewis
set/qth Western Pennsylvania
set/qra FN00FS
set/email john@example.net
set/homenode AI3I-16
set/password mynewpass
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

Useful related views:

- `show/station`
- `show/registered`

After changing your details, it is normal to verify them with:

```text
show/station
show/registered AI3I
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
send K3AJ Hello from pyCluster
msg K3AJ Hello from pyCluster
read 12
reply 12 Copy, thanks
talk K3AJ Good evening
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
- `show/msgstatus`
- `show/chat`
- `show/announce`
- `show/wcy`
- `show/wwv`
- `show/wx`

## 6. Filters

Users can filter the information they receive.

Examples:

```text
accept/spots 20m
reject/spots FT8
clear/spots
accept/rbn CQ
reject/rbn TEST
clear/rbn
```

Inspect filters with:

```text
show/filter
```

Filters are useful when:

- a band or mode is too noisy
- you only want certain activity types
- you want special handling for RBN-style spot traffic

## 7. Buddy Lists and User Data

Buddy list examples:

```text
set/buddy K3AJ
unset/buddy K3AJ
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
- cluster summary
- watch rules
- logging in to post
- editing their profile

See:

- [Public Web UI](public-web.md)

In general, the public web UI is more convenient when you want:

- visual filtering
- cluster overview tables
- watch rules and recent matches
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
