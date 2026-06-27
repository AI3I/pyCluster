# Public Web UI

The public web UI is the user-facing browser frontend for pyCluster.

Default local URL:

- `http://127.0.0.1:8081/`

## Backend State

Logged-in public web controls use the same backend database as telnet and the
System Operator console:

- spots, stats, leaderboards, bulletins, propagation, and cluster views are read from the shared store or shared runtime state
- profile fields write to `user_registry` and related `user_prefs`
- public filter presets, watch profiles, local watch rules, and recent watch matches are stored in the user's `public.presets` preference
- watch seeds are derived from database-backed buddy entries and positive spot filters
- common spot filter controls write a single compound rule to the same stored spot-filter table used by telnet `accept/spots` and `reject/spots`
- RBN visibility is controlled through the same access matrix and stored user preferences used elsewhere

Browser storage is used only as a convenience cache or for anonymous,
not-yet-authenticated UI state such as display toggles and temporary map/QTH
inputs.

## Main Pages

## Mobile Layout

The public web UI is responsive across desktop, phone, tablet, and foldable
layouts. On narrower screens:

- the spot map/table and sidebar stack vertically
- the stacked spot page scrolls so filters, metrics, watch, and operate panels remain reachable
- footer controls switch to a touch-friendly grid
- spot popups use a safe-area-aware position above the mobile footer
- profile, login, and registration modals scroll when the viewport is short

### Spots

Core live spot view.

Features:

- filter by band, mode, activity, DX continent, spotter continent, DX CQ zone, spotter CQ zone, comment tags, and text
- time-range filtering for `1h`, `3h`, `6h`, `12h`, `18h`, and `24h`

The web UI intentionally does not expose ITU-zone filtering. Logged-in filter choices are persisted into `filter_rules` as database-backed `accept/spots` rules so telnet and web sessions share the same effective spot filter.
- filter by spotter continent for logged-in operators; this is based on the station that posted the spot, not the spotted DX entity
- saved filter presets for logged-in users
- count of filtered vs total spots
- `RARE` badge support for selected entities
- `All` reset button when a filter is active

### Stats

Activity and leaderboard view for recent cluster traffic.

Shows:

- spot-rate chart
- daily spot history
- band-by-hour heat matrix
- band and continent activity bars
- top DX entities, spotters, and spotted DX calls

The page supports `1h`, `3h`, `6h`, `12h`, `18h`, and `24h` ranges. Longer
48-hour and 72-hour counters are intentionally not exposed because the rolling
spot totals are less useful at that scale in the current UI.

### Bulletins

Recent non-spot traffic from the cluster.

Shows:

- announcements
- chat/talk traffic
- WX bulletins
- WCY bulletins
- WWV bulletins

Features:

- category tabs for quick filtering
- most-recent-first ordering
- sender, time, and scope metadata
- automatic refresh after posting and on interval reloads

### Cluster

Cluster overview without pretending to know a global topology it has not observed.

Shows:

- direct links
- network summary
- known nodes
- family and version when explicitly learned

The network summary lists pyCluster nodes by callsign and lists legacy peers by
callsign with their learned family, rather than only showing aggregate family
counts.

### Propagation

Solar and geomagnetic dashboard.

Shows:

- solar flux, sunspots, A index, K index, X-ray class, and solar wind
- band-condition summaries from the solar feed, grouped as HF day, HF night, and VHF
- expanded VHF path labels such as `Sporadic E`, `Tropospheric`, and `Meteor Scatter`
- seven-day Planetary Kp values served through `/api/kp`

### Watch

Authenticated user watch/alerting page.

Features:

- only available after login
- cluster-backed watch seeds from buddies and positive spot filters
- local saved watch rules layered on top of those inherited cluster watch seeds
- recent matches
- hit counts
- saved watch profiles
- per-rule toast and sound controls

Logged-in watch rules, watch profiles, and recent watch matches are saved back
to the node database. Loading the public web from another browser will reload
those account-backed watch settings after login.

### Operate

Authenticated posting and user actions.

Features:

- available only after login
- permission-aware posting controls
- footer `Log In` popup for authentication
- footer `Register` popup for new registration requests
- footer `Edit Profile` popup after login

Profile fields:

- name
- location (QTH)
- grid square
- home node
- email address

MFA controls:

- view current MFA status and effective policy
- switch between email and authenticator-app MFA when both are available
- enable authenticator-app MFA with a QR code and manual setup key
- verify the active MFA method with a code
- disable user-level MFA and clear outstanding challenges

When email MFA is selected, `Verify` sends a code to the profile email address and asks for that code. When authenticator-app MFA is selected, `Verify` asks for the current app code. Login prompts say whether the code came from email or from the authenticator app.

## Posting Controls

Depending on node policy, a user may be allowed to post:

- DX spots
- RBN spots
- chat/talk
- announce
- WX
- WWV

Disabled actions in the UI reflect local policy.
